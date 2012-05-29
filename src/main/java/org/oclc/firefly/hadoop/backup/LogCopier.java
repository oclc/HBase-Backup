/*
 * Copyright (c) 2012 OCLC, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.oclc.firefly.hadoop.backup;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that continuosly archives HBase logs
 * It watches the /hbase/.logs and /hbase/.oldlogs directories for new files that may have appeared
 * and copies them to a location a configurable location.
 * 
 * It works by recursively scanning for HLogs within the logs directories in HBase and 
 * determines which files need to be copied. How frequent this is done can be configured.
 */
public class LogCopier {

    /** logger */
    private static final Logger LOG = LoggerFactory.getLogger(LogCopier.class);
    
    /** milliseconds per minute */
    private static final long MS_PER_MINUTE = 60000;
    
    /** milliseconds per day */
    private static final long ONE_DAY_MILLI = 86400000;
    
    /** default number of minutes between runs */
    private static final long DEFAULT_FREQUENCY = 10L;
    
    /** default number of threads */
    private static final long DEFAULT_NUM_THREADS = 10L;
    
    /** default buffer size 8KB */
    private static final int BUFFER_SIZE = 8192;

    /** the internal buffer */
    private byte[] buffer = new byte[BUFFER_SIZE];
    
    /** The path to copy the logs to */
    private Path destinationPath;
    
    /** number of minutes to sleep between runs */
    private long sleepMinutes;
    
    /** indicate thread should keep running */
    private boolean isRunning = false;
    
    /** Contains the last run's start date */
    private long runStartTime = 0L;
    
    /** Contains the last run's end date */
    private long runEndTime = 0L;
    
    /** the user name to run tasks as */
    private String username = System.getProperty("user.name");
    
    /** Number of logs copied in this session */
    private long totalSuccess = 0L;
    
    /** Number of logs that failed copied in this session */
    private long totalFailed = 0L;
    
    /** Number of logs copied in the last run */
    private long lastRunSuccess = 0L;
    
    /** Number of logs that failed copied in the last run */
    private long lastRunFailed = 0L;
    
    /** The number of days that a log is archived */
    private long daysToLive = 0L;
    
    /** number of copy threads */
    private long numberThreads = DEFAULT_NUM_THREADS;
    
    /** copy threads */
    List<Thread> threads = new ArrayList<Thread>();
    
    /**
     * Construct a new LogCopier
     * @param destination The destination of the log files
     * @param freqencyMinutes The time between runs in minutes
     */
    public LogCopier(String destination, long freqencyMinutes) {
        if (destination == null || destination.length() == 0) {
            throw new IllegalArgumentException("destination must be non-empty");
        }
        
        if (freqencyMinutes < 1) {
            throw new IllegalArgumentException("Minutes to sleep must be greater than 0");
        }
        
        destinationPath = new Path(destination);
        sleepMinutes = freqencyMinutes;
        isRunning = true;
    }
    
    /**
     * Main
     * @param args Command line arguments
     * @throws Exception If failed to read from file system
     */
    public static void main(String[] args) throws Exception {
        String destDirectory = null;
        long frequency = DEFAULT_FREQUENCY;
        long threads = DEFAULT_NUM_THREADS;
        long dtl = 0L;
        CommandLineParser parser = new PosixParser();
        CommandLine cmdline = null;
        
        // Parse command line options
        try {
            cmdline = parser.parse(getOptions(), args);
        } catch (org.apache.commons.cli.ParseException e) {
            System.out.println(e.getMessage());
            printOptions();
            System.exit(-1);
        }
        
        // Get command line options
        for (Option option : cmdline.getOptions()) {
            switch (option.getId()) {
            case 'd':
                destDirectory = option.getValue();
                break;
            case 'm':
                frequency = Long.parseLong(option.getValue());
                if (frequency <= 0) {
                    throw new IllegalArgumentException("Minutes must be greater than 0");
                }
                break;
            case 'l':
                dtl = Long.parseLong(option.getValue());
                if (dtl < 0) {
                    throw new IllegalArgumentException("Log days-to-live Must be non-negative");
                }
                break;
            case 't':
                threads = Long.parseLong(option.getValue());
                if (threads < 0) {
                    throw new IllegalArgumentException("Number of threads must be greater than 0");
                }
                break;
            default:
                throw new IllegalArgumentException("unexpected option " + option);
            }
        }
        
        LogCopier copier = new LogCopier(destDirectory, frequency);
        copier.setLogDaysToLive(dtl);
        copier.setNumberThreads(threads);

        LOG.info("--------------------------------------------------");
        LOG.info("Copy frequency    : " + copier.getCopyFrequency() + " minutes");
        LOG.info("Archive directory : " + copier.getArchiveDirectory());
        LOG.info("Log days to live  : " + copier.getLogDaysToLive() + " days");
        LOG.info("Copy threads      : " + copier.getNumberThreads());
        LOG.info("--------------------------------------------------");

        copier.run();
    }

    /**
     * Controls running loop
     */
    public void run() {
        Configuration conf;
        FileSystem fs;
        Path archivePath;
        
        while (isRunning) {
            try {
                startRunTimer();
                conf = HBaseConfiguration.create();
                fs = FileSystem.get(conf);
                archivePath = new Path(destinationPath, getTodayDateString());

                copyLogs(fs, archivePath, conf);
                endRunTimer();
                
                try {
                    putToSleep();
                } catch (InterruptedException ie) {
                    LOG.warn("Sleep interrupted by another thread. Resuming..", ie);
                }
            } catch (IOException e) {
                LOG.error("Failed to communicate with file system", e);
            }
        }
    }
    
    /**
     * Perform copy of logs to destination
     * @param fs The file system
     * @param archivePath The path to copy to
     * @param conf The configuration
     * @throws IOException thrown if failed to communicate with file system
     */
    private void copyLogs(FileSystem fs, Path archivePath, Configuration conf) throws IOException {
        String hbaseDir = conf.get(HConstants.HBASE_DIR);
        Path logsPath = new Path(hbaseDir, ".logs");
        Path oldLogsPath = new Path(hbaseDir, ".oldlogs");

        // Get a list of all log files that hbase exposes starting with .oldlogs, so that they can be copied first
        List<FileStatus> logFiles = getHLogs(fs, oldLogsPath, null);
        logFiles.addAll(getHLogs(fs, logsPath, null));
        
        // Over time, the list of archived files could get very large. The oldest start time is used to filter out
        // older archived HLogs. That way when we do comparisons we are only checking against the most recent files,
        // a smaller set.
        //long oldestFileStartTime = getOldestFileStartTime(logFiles);
        
        // Get the list of log files we have archived
        //List<FileStatus> copiedLogFiles = getHLogs(fs, destinationPath, oldestFileStartTime);
        List<FileStatus> copiedLogFiles = getHLogs(fs, destinationPath, null);
        
        // Get list of HLogs to copy
        List<FileStatus> newLogFiles = getNewLogFiles(fs, logFiles, copiedLogFiles);
        
        cleanOldLogsFromArchive(fs, copiedLogFiles);
        prepareLogsToBeReplaced(fs, newLogFiles, copiedLogFiles);
        
        lastRunSuccess = 0L;
        lastRunFailed = 0L;
        threads.clear();
        
        if (newLogFiles.size() > 0 && !fs.exists(archivePath)) {
            fs.mkdirs(archivePath);
        }

        // Split files among all threads and start start them
        // Improvement: use thread pool
        List<List<FileStatus>> split = splitFileList(newLogFiles, numberThreads);
        for (List<FileStatus> listFiles : split) {
            Thread t = new Thread(new Copier(fs, listFiles, archivePath));
            t.start();
            threads.add(t);
        }
        
        // Wait for threads to finish
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                LOG.error("Failed to join thread", e);
            }
        }
        
        totalSuccess += lastRunSuccess;
        totalFailed += lastRunFailed;
    }
    
    /**
     * Divide log files to copy among the number of threads
     * @param newLogFiles The list of logs files to split
     * @param numThreads The number of threads to split among
     * @return The split list of list of files
     */
    private List<List<FileStatus>> splitFileList(List<FileStatus> newLogFiles, long numThreads) {
        int idx = 0;
        long splits = numThreads;
        List<List<FileStatus>> ret = new ArrayList<List<FileStatus>>();
        
        if (newLogFiles.size() < numThreads) {
            splits = newLogFiles.size();
        }
        
        // Initialize the return array
        for (int i = 0; i < splits; i++) {
            ret.add(new ArrayList<FileStatus>());
        }

        // Divide files among threads
        for (FileStatus file : newLogFiles) {
            ret.get(idx).add(file);
            idx++;
            
            if (idx >= ret.size()) {
                idx = 0;
            }
        }
        
        return ret;
    }

    /**
     * Get the oldest start time from list of HLogs.
     * HLogs contain their start time at the end of the file name
     * @param logFiles The list of HLog files
     * @return The oldest time stamp
     */
    @SuppressWarnings("unused")
    private long getOldestFileStartTime(List<FileStatus> logFiles) {
        long ret = Long.MAX_VALUE;
        
        for (FileStatus file : logFiles) {
            String filePathString = file.getPath().toString();
            int iStartTime = filePathString.lastIndexOf('.');
            
            long logStartTime = Long.parseLong(filePathString.substring(iStartTime + 1));
            if (logStartTime < ret) {
                ret = logStartTime;
            }
        }
        
        return ret;
    }

    /**
     * Delete out-dated HLogs from archive. It goes through each HLog file in logFiles, and looks for it in
     * copiedLogFiles. If it finds it, it deletes it from the file system
     * @param fs The files system
     * @param copiedLogFiles The HLogs files in our archive
     * @throws IOException 
     */
    private void cleanOldLogsFromArchive(FileSystem fs, List<FileStatus> copiedLogFiles) throws IOException {
        long dtl = getLogDaysToLive();
        List<Path> deleteLogs = null;
        
        if (dtl > 0) {
            long deleteTimeMark = this.runStartTime - (dtl * ONE_DAY_MILLI);
            deleteLogs = new ArrayList<Path>();
            
            // Delete logs older than daysToLive
            for (int i = 0; i < copiedLogFiles.size(); i++) {
                FileStatus copiedLogFile = copiedLogFiles.get(i);
                String copiedLogFileName = copiedLogFile.getPath().toString();
                int iLogStartTime = copiedLogFileName.lastIndexOf('.');
                
                if (iLogStartTime > 0) {
                    try {
                        long logStartTime = Long.parseLong(copiedLogFileName.substring(iLogStartTime + 1));
                        if (logStartTime <= deleteTimeMark) {
                            deleteLogs.add(copiedLogFile.getPath());
                            copiedLogFiles.remove(i--);
                        }
                    } catch (NumberFormatException x) {
                        iLogStartTime = 0;
                    }
                }
                
                if (iLogStartTime <= 0) {
                    LOG.warn("Invalid log file: " + copiedLogFile.getPath());
                }
            }
            
            // Threaded delete, in case there are lots of deletes to perform
            // Improvement: use thread pool
            long numThreads = this.getNumberThreads();
            if (deleteLogs.size() < numThreads) {
                numThreads = deleteLogs.size();
            }
            
            if (numThreads > 0) {
                List<Thread> delThreads = new ArrayList<Thread>();
                int result = (int)(deleteLogs.size() / numThreads);
                int length = result;
                
                LOG.debug("Delete logs length: " + deleteLogs.size());
                
                // init threads
                for (int i = 0; i < numThreads; i++) {
                    int start = i * result;
                    
                    if (i + 1 == numThreads) {
                        length = deleteLogs.size() - start;
                    }
                    
                    LOG.debug("DThread " + i + ": start: " + start + " length: " + length);
                    
                    Runnable deleter = new Deleter(fs, deleteLogs, start, length);
                    Thread deleteThread = new Thread(deleter);
                    deleteThread.start();
                    
                    delThreads.add(deleteThread);
                }
                
                // wait for threads to finish
                for (Thread t : delThreads) {
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        LOG.error("Failed to join delete thread", e);
                    }
                }
            }
            
            // delete empty directories
            FileStatus[] dirs = fs.listStatus(this.destinationPath);
            if (dirs != null) {
                for (FileStatus dir : dirs) {
                    if (dir.isDir()) {
                        String name = dir.getPath().getName();
                        
                        try {
                            long dirTime = BackupUtils.LOG_COPIER_DATE_FORMAT.parse(name).getTime();
                            
                            if (dirTime <= deleteTimeMark) {
                                FileStatus[] logs = fs.listStatus(dir.getPath());
                                
                                if (logs == null || logs.length == 0) {
                                    LOG.info("Delete dir: " + dir.getPath());
                                    fs.delete(dir.getPath(), true);
                                }
                            }
                        } catch (ParseException pe) {
                            LOG.warn("Invalid LogCopier directory: " + dir.getPath());
                        }
                    } else {
                        LOG.warn("Invalid LogCopier file");
                    }
                }
            }
        }
    }
    
    /**
     * Delete out-dated HLogs from archive. It goes through each HLog file in logFiles, and looks for it in
     * copiedLogFiles. If it finds it, it deletes it from the file system
     * @param fs The files system
     * @param logFiles The new log files we have determined we need to copy to archive
     * @param copiedLogFiles The HLogs files in our archive
     */
    private void prepareLogsToBeReplaced(FileSystem fs, List<FileStatus> logFiles,
        List<FileStatus> copiedLogFiles) {
        for (FileStatus logFile : logFiles) {
            String logFileName = logFile.getPath().getName();
            
            for (int i = 0; i < copiedLogFiles.size(); i++) {
                FileStatus copiedLogFile = copiedLogFiles.get(i);
                String copiedLogFileName = copiedLogFile.getPath().getName();
                
                if (copiedLogFileName.equals(logFileName)) {
                    // There is a new version of the file
                    // delete old version from file system so that it can be replaced.
                    try {
                        LOG.info("Replacing out-dated archived HLog file " + copiedLogFile.getPath());
                        fs.delete(copiedLogFile.getPath(), false);
                        copiedLogFiles.remove(i--);
                    } catch (IOException e) {
                        LOG.error("Failed to delete out-dated archived HLog");
                    }
                    
                    break;
                }
            }
        }
    }

    /**
     * Get the HLog files that need to be archived. It goes through each HLog file in logFiles and checks if the
     * file name and size match any files in copiedLogFiles. If there are no matches, then the HLog can be archived
     * @param fs The file system
     * @param copiedLogFiles The HLog files in our archive
     * @param logFiles The list of HLog files HBase makes available
     * @return The list of files that we need to copy to the archive
     * @throws IOException if failed to communicate with file system
     */
    private List<FileStatus> getNewLogFiles(FileSystem fs, List<FileStatus> logFiles, List<FileStatus> copiedLogFiles)
        throws IOException {
        List<FileStatus> ret = new ArrayList<FileStatus>();
        
        for (FileStatus logFile : logFiles) {
            long logFileLen = logFile.getLen();
            
            if (logFileLen > 0) {
                boolean found = false;
                String logFileName = logFile.getPath().getName();

                for (FileStatus copiedLogFile : copiedLogFiles) {
                    String copiedLogFileName = copiedLogFile.getPath().getName();
                    long copiedLogFileLen = copiedLogFile.getLen();
                    
                    if (copiedLogFileName.equals(logFileName) && logFileLen == copiedLogFileLen) {
                        found = true;
                        break;
                    }
                }
                
                if (!found) {
                    ret.add(logFile);
                }
            }
        }
        
        return ret;
    }

    /**
     * Get a list of HLog files from given log directory.
     * @param fs The file system to scan
     * @param dir The log directory to scan
     * @param minStartTime The minimum start time of the files we get. Null to get all files
     * @return The list of files to copy
     * @throws IOException thrown if failed to read from file system
     */
    private List<FileStatus> getHLogs(FileSystem fs, Path dir, Long minStartTime) throws IOException {
        List<FileStatus> ret = new ArrayList<FileStatus>();
        LOG.debug("Scanning " + dir);

        FileStatus[] files = fs.listStatus(dir);
        if (files != null) {
            for (FileStatus file : files) {
                Path filePath = file.getPath();
                
                if (file.isDir()) {
                    ret.addAll(getHLogs(fs, filePath, minStartTime));
                } else {
                    String filePathString = filePath.toString();
                    int iStartTime = filePathString.lastIndexOf('.');
                    
                    if (iStartTime > 0) {
                        try {
                            long logStartTime = Long.parseLong(filePathString.substring(iStartTime + 1));
                            if (minStartTime == null || logStartTime >= minStartTime) {
                                LOG.debug("Found: " + filePathString);
                                ret.add(file);
                            }
                        } catch (NumberFormatException x) {
                            iStartTime = 0;
                        }
                    }
                    
                    if (iStartTime == 0) {
                        LOG.warn("Not an HLog: " + filePath);
                    }
                }
            }
        }
        
        return ret;
    }

    /**
     * Mark the start time of the last run
     */
    private void startRunTimer() {
        runStartTime = System.currentTimeMillis();
        runEndTime = 0L;
        LOG.info("Starting run on " + BackupUtils.PRETTY_DATE_FORMAT.format(new Date(runStartTime)));
    }
    
    /**
     * Mark the end time of the last run
     */
    private void endRunTimer() {
        runEndTime = System.currentTimeMillis();
        LOG.info("Completed on " + BackupUtils.PRETTY_DATE_FORMAT.format(new Date(runEndTime))
            + " (" + getHMSString(getLastRunLength()) + ") Results: [ last: "
            + lastRunSuccess + "/" + lastRunFailed + " total: " + totalSuccess + "/" + totalFailed + " ]");
    }
    
    /**
     * The time it took to complete the last run
     * @return The number of milliseconds it took to complete the last run. 0 if last run hasn't completed
     */
    private long getLastRunLength() {
        return (runStartTime == 0L || runEndTime == 0L) ? 0L : runEndTime - runStartTime;
    }
    
    /**
     * Return a string in the format "yyyy-MM-dd"
     * @return Todays date in string format
     */
    private String getTodayDateString() {
        return BackupUtils.LOG_COPIER_DATE_FORMAT.format(new Date());
    }
    
    /**
     * Get a string in the format "%d min, %d sec"
     * @param millis The number of milliseconds
     * @return The formatted string
     */
    private String getHMSString(long millis) {
        return String.format("%dm, %ds",
            TimeUnit.MILLISECONDS.toMinutes(millis),
            TimeUnit.MILLISECONDS.toSeconds(millis)
            - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
        );
    }
    
    /**
     * Put thread to sleep until next run
     * @throws InterruptedException if any thread has interrupted the current thread.
     * The interrupted status of the current thread is cleared when this exception is thrown.
     */
    private void putToSleep() throws InterruptedException {
        long maxTimeToSleep = MS_PER_MINUTE * sleepMinutes;
        long timeToSleep = maxTimeToSleep - getLastRunLength();

        if (timeToSleep > 0) {
            LOG.debug("Sleeping for " + getHMSString(timeToSleep));
            Thread.sleep(timeToSleep);
        }
    }

    /**
     * Get the archive directory
     * @return the archive directory string
     */
    public String getArchiveDirectory() {
        return this.destinationPath.toString();
    }

    /**
     * Get the frequency (in minutes) to copy logs
     * @return the copy time frequency
     */
    public long getCopyFrequency() {
        return this.sleepMinutes;
    }

    /**
     * Set the number of days that a log lives in the archive
     * @param dtl days to live
     * @throws IllegalArgumentException thrown if dtl is less than 0
     */
    public void setLogDaysToLive(long dtl) throws IllegalArgumentException {
        if (dtl < 0) {
            throw new IllegalArgumentException("dtl must be non-negative");
        }
        
        this.daysToLive = dtl;
    }
    
    /**
     * Get the number of days that a log lives in the archive
     * @return the number of days that a log lives in the archive
     */
    public long getLogDaysToLive() {
        return this.daysToLive;
    }
    
    /**
     * Set the number of threads that perform the copy work
     * @param numThreads The number of threads
     */
    private void setNumberThreads(long numThreads) {
        if (numThreads <= 0) {
            throw new IllegalArgumentException("Number of threads must be greater than zero");
        }
        
        this.numberThreads = numThreads;
    }
    
    /**
     * Get the number of copy threads
     * @return the number of copy threads
     */
    private long getNumberThreads() {
        return this.numberThreads;
    }
    
    /**
     * Returns the command-line options supported.
     * @return the command-line options
     */
    private static Options getOptions() {
        Options options = new Options();

        Option archiveDirectory = new Option("d", "archiveDir", true,
            "The root HLog archive directory. Example /path/to/log/archive");
        Option minutes = new Option("m", "minutes", true,
            "The frequency (in minutes) to copy new HLogs from HBase. Default: 10");
        Option ttl = new Option("l", "daysToLive", true,
            "Archived logs older than this many days will be deleted. Default is to not delete archived logs.");
        Option threads = new Option("t", "threads", true,
            "The number of copy threads");

        ttl.setRequired(false);
        archiveDirectory.setRequired(true);
        minutes.setRequired(false);
        threads.setRequired(false);
        
        options.addOption(ttl);
        options.addOption(archiveDirectory);
        options.addOption(minutes);
        options.addOption(threads);

        return options;
    }

    /**
     * Print the available options to the display.
     */
    private static void printOptions() {
        HelpFormatter formatter = new HelpFormatter();
        String header = "Tool to continuously archive HLogs";
        formatter.printHelp("LogCopier", header, getOptions(), "", true);
    }
    
    /**
     * Performs the copy step of LogCopier
     */
    public class Copier implements Runnable {
        
        /** The file system */
        private FileSystem fs;
        
        /** The log files to copy */
        private List<FileStatus> logFiles;
        
        /** The directory to save the files to */
        private Path archivePath;

        /**
         * Construct a new Copier
         * @param fs The file system
         * @param files The files to copy
         * @param path The archive path
         */
        public Copier(FileSystem fs, List<FileStatus> files, Path path) {
            this.fs = fs;
            this.logFiles = files;
            this.archivePath = path;
        }
        
        /**
         * Run copy step
         */
        @Override
        public void run() {
            copy();
        }
        
        /**
         * Copy all files
         */
        private void copy() {
            String hbaseDir = fs.getConf().get(HConstants.HBASE_DIR);
            Path logsPath = new Path(hbaseDir, ".logs");
            Path oldLogsPath = new Path(hbaseDir, ".oldlogs");
            
            for (FileStatus newLogFile : logFiles) {
                boolean copyFromOldLogs = false;
                Path newLogFilePath = newLogFile.getPath();
                Path logArchivePath = new Path(archivePath, newLogFilePath.getName());
                LOG.info("HLog: " + newLogFilePath);

                if (newLogFilePath.toString().startsWith(logsPath.toString())) {
                    try {
                        FileUtil.copy(fs, newLogFilePath, fs, logArchivePath, false, true, fs.getConf());
                        lastRunSuccess++;
                    } catch (IOException e) {
                        LOG.warn("Failed: " + newLogFilePath, e);
                        
                        copyFromOldLogs = true;
                        newLogFilePath = new Path(oldLogsPath, newLogFilePath.getName());
                        
                        LOG.info("Trying: " + newLogFilePath);
                    }
                } else {
                    copyFromOldLogs = true;
                }

                if (copyFromOldLogs) {
                    try {
                        // Use our copy function this time. It checks the trash for deleted files before giving up
                        BackupUtils.copy(fs, newLogFilePath, fs, logArchivePath, buffer, username,
                            fs.getDefaultReplication());
                        lastRunSuccess++;
                    } catch (Exception e) {
                        LOG.error(" Failed to copy " + newLogFilePath, e);
                        lastRunFailed++;
                    }
                }
            }
        }
        
    }
    
    /**
     * Class to delete files
     */
    class Deleter implements Runnable {
        
        /** the paths to delete */
        List<Path> paths;
        
        /** the file system */
        FileSystem fs;
        
        /** the start index in the paths array list */
        int startIdx;
        
        /** number of paths to delete */
        int count;

        /**
         * Construct a new Deleter
         * @param fs the file system
         * @param paths the paths to delete
         * @param startIdx the start index in paths
         * @param count the number of paths to delete 
         */
        public Deleter(FileSystem fs, List<Path> paths, int startIdx, int count) {
            this.fs = fs;
            this.paths = paths;
            this.startIdx = startIdx;
            this.count = count;
        }
        
        /**
         * Run thread
         */
        @Override
        public void run() {
            delete();
        }

        /**
         * Delete given files
         */
        private void delete() {
            for (int i = startIdx; i < startIdx + count; i++) {
                Path path = paths.get(i);
                
                try {
                    LOG.info("Delete: " + path);
                    fs.delete(path, false);
                } catch (IOException e) {
                    LOG.error("Failed to delete old HLog");
                }
            }
        }
        
    }
}
