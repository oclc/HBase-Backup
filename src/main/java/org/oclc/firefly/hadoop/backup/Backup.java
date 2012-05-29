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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase backup tool
 */
public class Backup {
    
    /** Class Logger */
    private static final Logger LOG = LoggerFactory.getLogger(Backup.class);
    
    /** name of hbase user config setting */
    public static final String JOBCONF_USER = "backup.hbase.user";
    
    /** name of hbase dest hdfs setting */
    public static final String JOBCONF_DEST_HDFS = "backup.copy.destination.hdfs";
    
    /** name of hbase dest path setting */
    public static final String JOBCONF_DEST_PATH = "backup.copy.destination.path";

    /** the replication factor for the copied files */
    public static final String JOBCONF_REPLICATION = "backup.copy.replication";
    
    /** The attempt id setting */
    public static final String JOBCONF_ATTEMPT_ID = "backup.copy.attempt.id";

    /** The backup directory */
    public static final String BACKUP_STORE_DIR = "/backup";
    
    /** mapper output directory */
    public static final String BACKUP_MAP_OUT_DIR = "/tmp/backup/output";
    
    /** mapper input directory */
    public static final String BACKUP_MAP_IN_DIR = "/tmp/backup/input";

    /** default buffer size 8KB */
    private static final int BUFFER_SIZE = 8192;
    
    /** the internal buffer */
    private byte[] buffer = new byte[BUFFER_SIZE];
    
    /** the source configuration */
    private Configuration srcConf = null;
    
    /** the destination configuration */
    private Configuration dstConf = null;
    
    /** the source file system */
    private FileSystem srcFs = null;
    
    /** the destination file system */
    private FileSystem dstFs = null;
    
    /** the user name to run tasks as */
    private String username = System.getProperty("user.name");

    /** the initial replication value */
    private int initialReplication = 1;
    
    /** the final replication factor */
    private int finalReplication = 0;
    
    /** the number of map tasks to run */
    private int numMapTasks = 1;
    
    /** A list of all the regions that have been copied */
    private List<HRegionInfo> copiedRegions = new ArrayList<HRegionInfo>();
    
    /** The backup directory Path */
    private Path backupDirectoryPath = null;
    
    /** the directory to store backups */
    private String storeDirectory = null;

    /**
     * Constructor
     * @param srcConf The source configuration
     * @param dstConf The destination configuration
     * @throws IOException If failed to get the file systems
     */
    public Backup(Configuration srcConf, Configuration dstConf) throws IOException {
        this.srcConf = srcConf;
        this.dstConf = dstConf;
        this.srcFs = FileSystem.get(srcConf);
        this.dstFs = FileSystem.get(dstConf);
        finalReplication = dstFs.getDefaultReplication();
    }
    
    /**
     * Entry point
     * @param args Command line arguments
     * @throws Exception exception
     */
    public static void main(String[] args) throws Exception {
        int initialReplication = 1;
        int finalReplication = 0;
        int numMaps = 2;
        int tries = 0;
        String tbl = null;
        String dest = null;
        String user = System.getProperty("user.name");
        Path destPath = null;
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
                dest = option.getValue();
                destPath = new Path(dest);
                if (!destPath.isAbsolute()) {
                    throw new IllegalArgumentException("Destination path must be an absolute path");
                }
                
                break;
            case 'm':
                numMaps = Integer.parseInt(option.getValue());
                if (numMaps <= 0) {
                    throw new IllegalArgumentException("Number of map tasks must be greater than zero.");
                }
                break;
            case 'n':
                tries = Integer.parseInt(option.getValue());
                if (tries < 0) {
                    throw new IllegalArgumentException(
                        "Maximum number of tries must be greater than or equal to zero.");
                }
                break;
            case 'f':
                finalReplication = Integer.parseInt(option.getValue());
                if (finalReplication <= 0) {
                    throw new IllegalArgumentException("Initial replication must be greater than zero.");
                }
                break;
            case 'r':
                initialReplication = Integer.parseInt(option.getValue());
                if (initialReplication <= 0) {
                    throw new IllegalArgumentException("Initial replication must be greater than zero.");
                }
                break;
            case 't':
                tbl = option.getValue();
                break;
            case 'u':
                user = option.getValue();
                break;
            default:
                throw new IllegalArgumentException("unexpected option " + option);
            }
        }
        
        String[] tables = null;
        if (tbl != null) {
            tables = tbl.split(",");
        }

        Configuration srcConf = HBaseConfiguration.create();
        Configuration dstConf = HBaseConfiguration.create();
        
        // This allows us to copy to a separate HDFS instance
        String destDir = null;
        if (dest != null) {
            destDir = destPath.toUri().getPath();
            String fsName = null;
            
            if (destDir != null && destDir.length() > 0) {
                LOG.debug("destination dfs: " + dest.substring(0, dest.length() - destDir.length()));
                fsName = dest.substring(0, dest.length() - destDir.length());
            } else {
                fsName = dest;
                destDir = null;
            }
            
            if (fsName != null && fsName.length() > 0) {
                dstConf.set("fs.default.name", fsName);
            }
        }
        
        Backup backup = new Backup(srcConf, dstConf);
        backup.setInitialReplication(initialReplication);
        backup.setFinalReplication(finalReplication);
        backup.setUsername(user);
        backup.setNumMapTasks(numMaps);
        if (destDir != null) {
            backup.setBackupStoreDirectory(destDir);
        }
        
        LOG.info("HBase backup tool");
        LOG.info("--------------------------------------------------");
        //LOG.info("Destination fs     : " + dstConf.get("fs.default.name"));
        LOG.info("Initial replication: " + backup.getInitialReplication());
        LOG.info("Final replication  : " + backup.getFinalReplication());
        LOG.info("Number of attempts : " + ((tries == 0) ? "Until nothing left to copy" : tries));
        LOG.info("Username           : " + backup.getUsername());
        LOG.info("Number map tasks   : " + backup.getNumMapTasks());
        LOG.info("Backup store path  : " + backup.getBackupStoreDirectory());
        LOG.info("--------------------------------------------------");
        
        boolean success = backup.doMajorCopy(tables, tries);

        LOG.info("--------------------------------------------------");
        if (success) {
            LOG.info("Backup located at: " + backup.getBackupDirectoryPath());
            LOG.info("Backup complete");
        } else {
            LOG.info("Files located at: " + backup.getBackupDirectoryPath());
            LOG.info("Backup failed");
        }

        System.exit(success ? 0 : -1);
    }

    /**
     * Performs a complete copy of the source hbase to the given destination
     * @param tables The names of the tables to backup
     * @param maxTries The maximum number of times to try to copy regions.
     * @return True if successful, false otherwise
     * @throws IOException If failed to interact with Hadoop
     * @throws ClassNotFoundException 
     * @throws InterruptedException 
     */
    public boolean doMajorCopy(String[] tables, int maxTries)
        throws IOException, InterruptedException, ClassNotFoundException {
        boolean ret = false;
        String username = getUsername();
        short replication = (short)getInitialReplication();
        
        // Get a list of regions from HBase
        // Then filter out the regions we are not extracting, and group them by table
        List<CatalogRow> regions  = getHBaseRegions(srcConf);
        Map<String, List<CatalogRow>> filtered = groupAndFilter(regions, tables);
        List<Pair<String, HRegionInfo>> mapperInput = new ArrayList<Pair<String, HRegionInfo>>();
        
        // Prepare the input for the mappers to use
        // This creates a list of region server and region pairs
        LOG.info("Exporting the following tables:");
        for (Entry<String, List<CatalogRow>> entry : filtered.entrySet()) {
            String tablename = entry.getKey();
            List<CatalogRow> rows = entry.getValue();
            
            LOG.info(". " + tablename);
            
            for (CatalogRow r : rows) {
                String regionServer = r.getHost() + ":" + r.getPort();
                HRegionInfo region = r.getHRegionInfo();
                mapperInput.add(Pair.newPair(regionServer, region));
            }
        }

        // Make sure we write to a directory that does not exist
        backupDirectoryPath = createBackupDirectory(getCurrentDateString());
        LOG.info("Starting backup path: " + backupDirectoryPath);
        
        // Copy the .tableinfo files for the tables we are extracting
        // These files are not copied by the MR job as it only focuses on regions
        List<FileStatus> tableInfoFiles = getTableInfoFiles(srcFs, filtered);
        for (FileStatus file : tableInfoFiles) {
            Path srcFilePath = file.getPath();
            Path relPath = new Path(BackupUtils.getFsRelativePath(srcFs, srcFilePath));
            Path dstFilePath = new Path(backupDirectoryPath.toString() + relPath.toString());
            BackupUtils.copy(srcFs, srcFilePath, dstFs, dstFilePath, buffer, username, replication);
        }
        
        // Dispatch MR job and monitor
        // Retry regions if necessary
        if (mapperInput.size() > 0) {
            int tries = 0;
            
            while (!ret && (maxTries == 0 || tries < maxTries)) {
                if (getNumMapTasks() > mapperInput.size()) {
                    setNumMapTasks(mapperInput.size());
                    LOG.info("Not enough regions. Reducing number of map tasks");
                }

                // Generate a list of mapper input files and create job
                List<Path> sourceFiles = createMapperInputSequenceFiles(mapperInput, getNumMapTasks(), srcFs, tries);
                Job job = createMRJob(srcConf, dstConf, sourceFiles, backupDirectoryPath, tries);

                LOG.info(job.getJobName());
                LOG.info("--------------------------------------------------");
                LOG.info("Number of regions  : " + mapperInput.size());
                LOG.info("Number of map tasks: " + getNumMapTasks());
                LOG.info("Mapper input path  : " + getMapInputDirectory(tries));
                LOG.info("Mapper output path : " + FileOutputFormat.getOutputPath(job));
                LOG.info("--------------------------------------------------");
                
                job.waitForCompletion(true);
                if (job.isSuccessful()) {
                    // Check if any regions failed
                    Counters counters = job.getCounters();
                    Counter failedCounter = counters.findCounter("Backup", "FailedRegions");
                    long failed = failedCounter.getValue();
                    
                    if (failed > 0) {
                        LOG.info("Number of failed regions: " + failed + ".");
                        
                        // get a fresh list of regions to copy
                        List<Pair<String, HRegionInfo>> failedRegions = getFailedRegions(srcFs, srcConf, tries);
                        addCopiedRegions(mapperInput, failedRegions);
                        mapperInput = getRemainingRegions(mapperInput, tables);
                        
                        for (Pair<String, HRegionInfo> pair : mapperInput) {
                            LOG.info("Retry: " + pair.getSecond());
                        }
                        
                        if (mapperInput.size() == 0) {
                            ret = true;
                            backupDirectoryPath = appendEndTime(backupDirectoryPath);

                            LOG.warn("No regions left to copy, but expected to copy more. "
                                + "Please inspect logs/files manually for errors");
                        }
                    } else {
                        ret = true;
                        
                        addCopiedRegions(mapperInput, null);
                        backupDirectoryPath = appendEndTime(backupDirectoryPath);
                        LOG.info("MR job finished successfully");
                    }
                } else {
                    LOG.error("An unexpected error occurred during the MR job. Please see MR logs.");
                    break;
                }
                
                tries++;
            }
            
            if (ret) {
                if (verifyCopiedRegions()) {
                    LOG.info("Verification passed succesfully");
                } else {
                    ret = false;
                    LOG.info("Verification failed. Please inspect errors manually");
                }
            } else {
                LOG.info("No attempts left. Try setting -n to a higher value, or setting it to 0");
            }
        }
        
        if (ret) {
            // Set replication factor of backup directory to default.
            // This may not be the best solution, but let built-in shell take care of it
            // because it can do it recursively with out us having to rediscover all the files
            short finalReplication = (short)getFinalReplication();
            
            if (replication != finalReplication) {
                FsShell shell = new FsShell(dstConf);
                String[] repArgs = { "-setrep", "-R", "-w", "" + finalReplication, backupDirectoryPath.toString() };
                
                try {
                    LOG.info("Setting final replication factor of backup files to " + finalReplication);
                    shell.run(repArgs);
                } catch (Exception e) {
                    LOG.warn("Could not set replication factor of backup files to " + finalReplication);
                }
            }
        }
        
        return ret;
    }

    /**
     * Verify the copied regions
     * @return True if verification succeeds. False otherwise
     */
    private boolean verifyCopiedRegions() {
        boolean ret = true;
        Map<String, List<HRegionInfo>> tableRegions = groupRegionsByTableName(copiedRegions);
        
        for (Map.Entry<String, List<HRegionInfo>> entry : tableRegions.entrySet()) {
            HRegionInfo prevRegion = null;
            String tableName = entry.getKey();
            List<HRegionInfo> regions = entry.getValue();
            
            // sort regions from start to end region
            Collections.sort(regions);
            LOG.info("Checking table: " + tableName);
            
            for (int i = 0; i < regions.size(); i++) {
                HRegionInfo currRegion = regions.get(i);
                LOG.info("  " + i + ": " + currRegion);
                
                if (regions.size() == 1) {
                    // Single region. Start and end key should be empty
                    if (currRegion.getStartKey().length != 0 || currRegion.getEndKey().length != 0) {
                        ret = false;
                        LOG.error(tableName + ": Single region, expecting start and keys to be empty");
                        LOG.error(" " + currRegion);
                    }
                } else {
                    if (i == 0) {
                        // First region. Current start key should be empty
                        if (currRegion.getStartKey().length != 0) {
                            ret = false;
                            LOG.error(tableName + ": First region. Expecting start key to be empty");
                            LOG.error(" " + currRegion);
                        }
                    } else {
                        // Last region or middle region. Current start key should equals previous end key
                        if (Bytes.compareTo(currRegion.getStartKey(), prevRegion.getEndKey()) != 0) {
                            ret = false;
                            LOG.error(tableName + ": Missing region. "
                                + "End key and start key of adjacent regions don't match");
                            LOG.error("  left: " + prevRegion);
                            LOG.error(" right: " + currRegion);
                        }

                        if (i == regions.size() - 1) {
                            // Last region. The current end key should be empty
                            if (currRegion.getEndKey().length != 0) {
                                ret = false;
                                LOG.error(tableName + ": Last region. Expecting end key to be empty");
                                LOG.error(" " + currRegion);
                            }
                        }
                    }
                }

                prevRegion = currRegion;
            }
        }
        
        return ret;
    }

    /**
     * Group the given region list by table names
     * @param regions The list of regions to group
     * @return A map where the key is the table name and value is a list of all regions for that table
     */
    private Map<String, List<HRegionInfo>> groupRegionsByTableName(List<HRegionInfo> regions) {
        Map<String, List<HRegionInfo>> ret = new TreeMap<String, List<HRegionInfo>>();
        
        for (HRegionInfo region : regions) {
            String tableName = region.getTableNameAsString();
            List<HRegionInfo> value = ret.get(tableName);
            
            if (value == null) {
                value = new ArrayList<HRegionInfo>();
            }
            
            value.add(region);
            ret.put(tableName, value);
        }

        return ret;
    }

    /**
     * Rename backup directory to contain start and end date of backup
     * @param dstPath The current path to backup directory
     * @return The final name of the backup directory
     * @throws IOException Thrown if failed to rename backup directory
     */
    private Path appendEndTime(Path dstPath) throws IOException {
        Path finalBackupPath = new Path(dstPath.toString() + "-" + getCurrentDateString());
        dstFs.rename(dstPath, finalBackupPath);
        return finalBackupPath;
    }

    /**
     * Add copied regions to global list of copied regions making sure not to add failed regions
     * @param inputRegions The list of regions that were sent to mappers to copy
     * @param failedRegions The list of regions that failed to be copied. Can be null to indicate no failed regions
     */
    private void addCopiedRegions(List<Pair<String, HRegionInfo>> inputRegions,
        List<Pair<String, HRegionInfo>> failedRegions) {

        for (Pair<String, HRegionInfo> pair : inputRegions) {
            boolean hasFailed = false;
            HRegionInfo region = pair.getSecond();
        
            if (failedRegions != null) {
                // search for this region among failed regions
                for (int i = 0; i < failedRegions.size() && !hasFailed; i++) {
                    HRegionInfo failedRegion = failedRegions.get(i).getSecond();
                    
                    if (region.equals(failedRegion)) {
                        hasFailed = true;
                    }
                }
            }
            
            // Add to list of copied regions only if copy didn't fail
            if (!hasFailed) {
                copiedRegions.add(region);
            }
        }
    }

    /**
     * Get a list of regions to copy for next attempt
     * @param oldRegions The list of all regions that were tried in the last MR run
     * @param tables The table we are backing up
     * @return The list of remaining regions
     * @throws IOException Thrown if failed to read from HBase
     */
    private List<Pair<String, HRegionInfo>> getRemainingRegions(List<Pair<String, HRegionInfo>> oldRegions,
        String[] tables) throws IOException {
        List<Pair<String, HRegionInfo>> ret = new ArrayList<Pair<String, HRegionInfo>>();

        // Get the most current list of regions in .META.
        List<CatalogRow> regionsInMeta  = getHBaseRegions(srcConf);
        
        LOG.info("Calculating remaining regions");
        
        // Remove those regions from regionsInMeta which are already in copiedRegions (should be most of them)
        // This steps retains only the regions which we have yet to copy
        for (int i = 0; i < regionsInMeta.size(); i++) {
            HRegionInfo region = regionsInMeta.get(i).getHRegionInfo();
            
            if (copiedRegions.contains(region)) {
                regionsInMeta.remove(i);
                --i;
            }
        }
        
        // Remove regions from regionsInMeta that are daughters of any region in copiedRegions
        for (int i = 0; i < regionsInMeta.size(); i++) {
            CatalogRow r = regionsInMeta.get(i);
            
            for (HRegionInfo copiedRegion : copiedRegions) {
                if (BackupUtils.regionContains(copiedRegion, r.getHRegionInfo())) {
                    LOG.info("Daughter region : " + r.getHRegionInfo());
                    LOG.info("  Copied parent : " + copiedRegion);
                    
                    regionsInMeta.remove(i);
                    --i;
                    break;
                }
            }
        }
        
        // Get only the regions for the tables we are extracting
        Map<String, List<CatalogRow>> filtered = groupAndFilter(regionsInMeta, tables);
        for (Entry<String, List<CatalogRow>> entry : filtered.entrySet()) {
            List<CatalogRow> rows = entry.getValue();

            for (CatalogRow r : rows) {
                String regionServer = r.getHost() + ":" + r.getPort();
                HRegionInfo region = r.getHRegionInfo();
                ret.add(Pair.newPair(regionServer, region));
            }
        }
        
        return ret;
    }

    /**
     * Reads files from retry directory and collects input for the
     * next set of mappers to run
     * @param fs The filesystem to read from
     * @param conf The configuration object
     * @param id The mapper id
     * @return The list of mapper inputs
     * @throws IOException thrown if failed to read from filesystem
     */
    private List<Pair<String, HRegionInfo>> getFailedRegions(FileSystem fs, Configuration conf, int id)
        throws IOException {
        List<Pair<String, HRegionInfo>> ret = new ArrayList<Pair<String, HRegionInfo>>();
        Text rserver = new Text();        
        Path retryPath = new Path(getMapOutputDirectory(id));
        FileStatus[] list = fs.listStatus(retryPath);

        if (list != null) {
            LOG.info("Getting failed regions");
            
            for (FileStatus file : list) {
                Path filePath = file.getPath();
                if (filePath.getName().startsWith("part-")) {
                    LOG.debug("Retry file: " + filePath);
                    
                    SequenceFile.Reader reader = new SequenceFile.Reader(fs, filePath, conf);
                    while (reader.next(rserver)) {
                        HRegionInfo rinfo = new HRegionInfo();
                        reader.getCurrentValue(rinfo);

                        LOG.info(rinfo.toString());
                        ret.add(Pair.newPair(rserver.toString(), rinfo));
                    }
                    
                    try {
                        reader.close();
                    } catch (Exception e) {
                        // Ignore error
                    }
                }
            }
        }
        
        return ret;
    }

    /**
     * Get the MR job configuration object already configured
     * @param srcConf The hdfs source configuration
     * @param dstConf The hdfs destination configuration
     * @param sourceFiles The mapper input files
     * @param dstPath The backup path
     * @param id The internal id of this job 
     * @return The configured object
     * @throws IOException If fails to read from file system
     */
    private Job createMRJob(Configuration srcConf, Configuration dstConf, List<Path> sourceFiles,
        Path dstPath, int id) throws IOException {
        srcConf.set(JOBCONF_DEST_HDFS, dstConf.get("fs.default.name"));
        srcConf.set(JOBCONF_DEST_PATH, dstPath.toString());
        srcConf.set(JOBCONF_USER, getUsername());
        srcConf.setInt(JOBCONF_ATTEMPT_ID, id);
        srcConf.setInt(JOBCONF_REPLICATION, getInitialReplication());
        srcConf.setInt("mapred.map.tasks", getNumMapTasks());

        // Don't want multiple mappers copying the same file to the same location
        srcConf.setBoolean("mapred.map.tasks.speculative.execution", false);
        
        Job job = new Job(srcConf);
        job.setJobName("Backup " + dstPath.getName() + " (Attempt " + (id + 1) + ")");
        job.setJarByClass(Backup.class);
        job.setMapperClass(CopyRegionMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(HRegionInfo.class);

        job.setNumReduceTasks(0);
        job.setInputFormatClass(BackupInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        for (Path file : sourceFiles) {
            LOG.debug("Map input: " + file.toString());
            FileInputFormat.addInputPath(job, file);
        }

        // Set output path, delete first if it exists
        Path outputPath = new Path(getMapOutputDirectory(id));
        srcFs.delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        return job;
    }
    
    /**
     * Create mapper input files containing their paths to copy
     * @param mapperInput The list of files that the copy mappers should copy
     * @param numMapTasks The number of map tasks
     * @param fs The file system to write to
     * @param id The mapper id
     * @return The list of input files for a a mapper
     * @throws IOException If we fail to create input files
     */
    private List<Path> createMapperInputSequenceFiles(List<Pair<String, HRegionInfo>> mapperInput,
        int numMapTasks, FileSystem fs, int id) throws IOException {
        int idx = 0;
        List<Path> paths = new ArrayList<Path>();
        List<SequenceFile.Writer> writers = new ArrayList<SequenceFile.Writer>();
        String inputDir = getMapInputDirectory(id);
        
        // delete this directory if already exists
        fs.delete(new Path(inputDir), true);
        
        // each mapper gets an input file
        for (int i = 0; i < numMapTasks; i++) {
            // open the input file for writing
            Path mapInputFile = new Path(inputDir + "/mapper-input-" + i + ".txt");
            fs.delete(mapInputFile, false);

            SequenceFile.Writer writer = SequenceFile.createWriter(fs, fs.getConf(), mapInputFile, Text.class,
                HRegionInfo.class, SequenceFile.CompressionType.NONE);
            
            LOG.debug("Mapper input: " + mapInputFile);
            
            paths.add(mapInputFile);
            writers.add(writer);
        }
        
        // Assign copy paths to mappers
        for (Pair<String, HRegionInfo> pair : mapperInput) {
            Text key = new Text(pair.getFirst());
            HRegionInfo value = new HRegionInfo(pair.getSecond());
            
            LOG.debug("Appending " + key + ", " + value.getEncodedName());
            writers.get(idx).append(key, value);

            idx++;
            if (idx >= writers.size()) {
                idx = 0;
            }
        }
        
        // close writers
        for (SequenceFile.Writer writer : writers) {
            try {
                writer.sync();
                writer.close();
            } catch (Exception e) {
                // nothing to do here
            }
        }
        
        return paths;
    }
    
    /**
     * Create a new temporary directory and return its path.
     * Guarantees that it does not exist at the moment that this function is called.
     * @param startTime The date time string
     * @return The Path to the temporary directory
     * @throws IOException When failed to read file system
     */
    private Path createBackupDirectory(String startTime) throws IOException {
        Path ret;

        String parent = getBackupStoreDirectory();
        ret = new Path(parent, "bak-" + startTime);
        dstFs.mkdirs(ret);
        
        return ret;
    }

    /**
     * Returns a string of the current date time in the format yyyyMMdd.kkmmss.SSS
     * @return The date time string
     */
    private String getCurrentDateString() {
        return BackupUtils.BACKUP_DATE_FORMAT.format(new Date());
    }
    
    /**
     * Group a list of CatalogRow by table name
     * @param regions The list CatalogRow
     * @param tables The names of the tables to keep. Anything not listed is filtered out
     * @return A map where the key is the table name and value is a list of CatalogRow objects
     */
    public Map<String, List<CatalogRow>> groupAndFilter(List<CatalogRow> regions, String[] tables) {
        Map<String, List<CatalogRow>> ret = new HashMap<String, List<CatalogRow>>();
        List<HRegionInfo> daughterRegions = new ArrayList<HRegionInfo>();
        
        // When regions split, the parent region still contains the actual files with the data
        // The daughter regions only contain references, which we will not copy
        // We have to be careful with this case as the parent region could be deleted at any point during the backup
        for (CatalogRow r : regions) {
            HRegionInfo region = r.getHRegionInfo();
            
            if (region.isSplit()) {
                LOG.warn("Region is split: " + region);
                if (!region.isOffline()) {
                    LOG.warn(" But region is not offline");
                }
                
                HRegionInfo splitA = r.getSplitA();
                HRegionInfo splitB = r.getSplitB();
                
                if (splitA != null) {
                    LOG.warn("        Split A: " + splitA);
                    daughterRegions.add(splitA);
                }
                
                if (splitB != null) {
                    LOG.warn("        Split B: " + splitB);
                    daughterRegions.add(splitB);
                }
            }
        }
        
        for (CatalogRow r : regions) {
            HRegionInfo region = r.getHRegionInfo();
            
            // Leave out daughter regions
            if (!daughterRegions.contains(region)) {
                String tName = region.getTableNameAsString();
                
                if (tables == null || ArrayUtils.contains(tables, tName)) {
                    List<CatalogRow> value = ret.get(tName);
                    if (value == null) {
                        value = new ArrayList<CatalogRow>();
                    }
                    
                    value.add(r);
                    ret.put(tName, value);
                }
            } else {
                LOG.warn("Filtered daughter region: " + region);
            }
        }
        
        return ret;
    }
    
    /**
     * Get the list of files to copy. 
     * @param fs The file system to get file from
     * @param tableRegions the table regions for which to look files
     * @return A list of file names
     * @throws IOException When failed to communicate with filesystem
     */
    public List<FileStatus> getTableInfoFiles(FileSystem fs, Map<String, List<CatalogRow>> tableRegions)
        throws IOException {
        List<FileStatus> ret = new ArrayList<FileStatus>();
        String rootDir = fs.getConf().get(HConstants.HBASE_DIR);

        // Get list of files to copy one table at a time
        for (Map.Entry<String, List<CatalogRow>> entry : tableRegions.entrySet()) {
            String tableName = entry.getKey();
            Path tableDirPath = new Path(rootDir, tableName);

            // Add .tableinfo to list of files to cppy
            try {
                FileStatus tableInfoFile = BackupUtils.getTableInfoPath(fs, tableDirPath);
                ret.add(tableInfoFile);
            } catch (FileNotFoundException e) {
                // Not sure what to do if we can't find this file
                LOG.warn("No .tableinfo file found for table " + tableName);
            }
        }
        
        return ret;
    }
    
    /**
     * Get the list of files to copy. 
     * @param fs The file system to get file from
     * @param tableRegions the table regions for which to look files
     * @return A list of file names
     * @throws IOException When failed to communicate with filesystem
     */
    public List<FileStatus> getListOfFiles(FileSystem fs, Map<String, List<CatalogRow>> tableRegions)
        throws IOException {
        List<FileStatus> ret = new ArrayList<FileStatus>();
        String rootDir = fs.getConf().get(HConstants.HBASE_DIR);

        // Get list of files to copy one table at a time
        for (Map.Entry<String, List<CatalogRow>> entry : tableRegions.entrySet()) {
            String tableName = entry.getKey();
            Path tableDirPath = new Path(rootDir, tableName);

            // Add .tableinfo to list of files to cppy
            try {
                FileStatus tableInfoFile = BackupUtils.getTableInfoPath(fs, tableDirPath);
                ret.add(tableInfoFile);
            } catch (FileNotFoundException e) {
                // Not sure what to do if we can't find this file
                LOG.warn("No .tableinfo file found for table " + tableName);
            }

            // Get table descriptor so we may get information about the table we are extracting
            HTableDescriptor tDesc = FSTableDescriptors.getTableDescriptor(fs, tableDirPath);
            
            if (tDesc == null) {
                throw new TableNotFoundException("Could not get HTableDescriptor for table " + tableName);
            }

            // Need to find out what column families this table has
            // so that we may generate paths to the files we are copying
            HColumnDescriptor[] columnFamilies = tDesc.getColumnFamilies();
            
            List<CatalogRow> regions = entry.getValue();
            for (CatalogRow r : regions) {
                HRegionInfo info = r.getHRegionInfo();
                String regionName = info.getEncodedName();

                // Add .regioninfo to list of files to cppy
                Path regionDirPath = new Path(tableDirPath, regionName);
                Path regionInfoFilePath = new Path(regionDirPath, HRegion.REGIONINFO_FILE);
                
                try {
                    FileStatus regionInfoFile = fs.getFileStatus(regionInfoFilePath);
                    ret.add(regionInfoFile);
                } catch (FileNotFoundException e) {
                    // Not sure what to do if we can't find this file
                    LOG.warn("No .regioninfo file found for region " + tableName + "/" + regionName);
                }

                for (HColumnDescriptor col : columnFamilies) {
                    String family = col.getNameAsString();
                    Path regionFamilyDirPath = new Path(regionDirPath, family);

                    try {
                        // Add column family directories to make sure
                        // they get copied should they be empty
                        FileStatus dirStatus = fs.getFileStatus(regionFamilyDirPath);
                        ret.add(dirStatus);
                        
                        // Finally, get all the files under this column family
                        FileStatus[] statusList = fs.listStatus(regionFamilyDirPath);
                        if (statusList != null) {
                            for (FileStatus status : statusList) {
                                ret.add(status);
                            }
                        }
                    } catch (FileNotFoundException e) {
                        LOG.warn("Expecting region family directory '" + regionFamilyDirPath + "' but not found");
                    }
                }
            }
        }
        
        return ret;
    }
    
    /**
     * Get all regions listed in .META.
     * @param conf the hbase configuration
     * @return List of regions
     * @throws IOException 
     */
    public static List<CatalogRow> getHBaseRegions(Configuration conf) throws IOException {
        List<CatalogRow> ret = null;
        
        HTable meta = getMetaTable(conf);
        Scan metaScanner = new Scan();
        metaScanner.addFamily(Bytes.toBytes("info"));
        ResultScanner metaResults = meta.getScanner(metaScanner);
        
        try {
            ret = new ArrayList<CatalogRow>();
            for (Result r : metaResults) {
                CatalogRow row = new CatalogRow(r);
                ret.add(row);
            }
        } finally {
            metaResults.close();
        }
        
        return ret;
    }
    
    /**
     * Get root table HTable object
     * @param config the hbase config
     * @return The root HTable object
     */
    public static HTable getRootTable(Configuration config) {
        HTable table = null;

        try {
            table = new HTable(config, HConstants.ROOT_TABLE_NAME);
        } catch (IOException e) {
            LOG.error("Could not instantiate -ROOT- HTable");
        }
        
        return table;
    }
    
    /**
     * Get root table HTable object
     * @param config the hbase config
     * @return The root HTable object
     */
    public static HTable getMetaTable(Configuration config) {
        HTable table = null;

        try {
            table = new HTable(config, HConstants.META_TABLE_NAME);
        } catch (IOException e) {
            LOG.error("Could not instantiate .META. HTable");
        }
        
        return table;
    }
    
    /**
     * Get the backup directory path
     * @return the backup directory path
     */
    public Path getBackupDirectoryPath() {
        return this.backupDirectoryPath;
    }
    
    /**
     * Set the name of user to run as
     * @param username the username to set
     * @throws IllegalArgumentException If username is null or empty
     */
    public void setUsername(String username) throws IllegalArgumentException {
        if (username == null || username.length() == 0) {
            throw new IllegalArgumentException("Username cannot be null or empty");
        }
        
        this.username = username;
    }

    /**
     * Set the initial replication factor
     * @param replication the replication to set
     * @throws IllegalArgumentException Thrown if initial replication is less than or equal to zero
     */
    public void setInitialReplication(int replication) throws IllegalArgumentException {
        if (finalReplication <= 0) {
            throw new IllegalArgumentException("Initial replication must be greater than zero");
        }
        
        this.initialReplication = replication;
    }
    
    /**
     * Set the final replication factor. A value of 0 sets it to configuration default.
     * @param finalReplication The final replication value.
     * @throws IllegalArgumentException Thrown if value is less than 0
     */
    public void setFinalReplication(int finalReplication) throws IllegalArgumentException {
        if (finalReplication < 0) {
            throw new IllegalArgumentException("Final replication must be non-negative");
        } else if (finalReplication > 0) {
            this.finalReplication = finalReplication;
        } else {
            this.finalReplication = dstFs.getDefaultReplication();
        }
    }
    
    /**
     * Get the final replication factor
     * @return The final replication factor
     */
    public int getFinalReplication() {
        return this.finalReplication;
    }
    
    /**
     * Set the number of map tasks to run
     * @param numMapTasks the numMapTasks to set
     * @throws IllegalArgumentException If value is not greater than zero
     */
    public void setNumMapTasks(int numMapTasks) throws IllegalArgumentException {
        if (numMapTasks <= 0) {
            throw new IllegalArgumentException("Number of map tasks must be greater than zero");
        }
        
        this.numMapTasks = numMapTasks;
    }
    
    /**
     * Get the name of user to run as
     * @return the username
     */
    public String getUsername() {
        return username;
    }
    
    /**
     * Get the backup storage directory
     * @return the backup storage directory
     */
    public String getBackupStoreDirectory() {
        String ret = this.storeDirectory;
        
        if (ret == null) {
            ret = BACKUP_STORE_DIR;
            String user = getUsername();
            
            if (user != null) {
                ret = "/user/" + user + ret;
            }
        }
        
        return dstFs.getUri().toString() + ret;
    }
    
    /**
     * Set the backup storage directory for backups
     * @param storeDir Path to store directory
     */
    public void setBackupStoreDirectory(String storeDir) {
        if (storeDir == null) {
            throw new NullPointerException("Backup store directory is null");
        } else if (storeDir.trim().length() == 0) {
            throw new IllegalArgumentException("Invalid DFS path");
        }

        this.storeDirectory = storeDir;
    }
    
    /**
     * Get the map output directory
     * @param id The mapper id
     * @return The map output directory
     */
    public String getMapOutputDirectory(int id) {
        String ret = BACKUP_MAP_OUT_DIR + "/output-" + id;
        String user = getUsername();
        
        if (user != null) {
            ret = "/user/" + user + ret;
        }
        
        return ret;
    }
    
    /**
     * Get the map input directory
     * @param id The mapper id
     * @return The map output directory
     */
    public String getMapInputDirectory(int id) {
        String ret = BACKUP_MAP_IN_DIR + "/input-" + id;
        String user = getUsername();
        
        if (user != null) {
            ret = "/user/" + user + ret;
        }
        
        return ret;
    }

    /**
     * Get the initial replication factor
     * @return the initialReplication
     */
    public int getInitialReplication() {
        return initialReplication;
    }

    /**
     * Get the number of map tasks to run
     * @return the numMapTasks
     */
    public int getNumMapTasks() {
        return numMapTasks;
    }
    
    /**
     * Returns the command-line options supported.
     *
     * @return the command-line options
     */
    private static Options getOptions() {
        Options options = new Options();

        Option initialreplication = new Option("r", "initialReplication", true,
            "The initial replication factor of copied files. " 
            + "Default is 1 because it cuts down the copy time window which helps reduce errors. "
            + "Note: this can cause problems if a block is lost while backup is running.");
        Option finalReplication = new Option("f", "finalReplication", true, 
            "The final replication factor of copied files. Default is destination config default. "
            + "This is desirable if initial replication is set low.");
        Option tries = new Option("n", "tries", true,
            "The maximum number of times to attempt to copy regions. Default: 0 (Try until nothing left to copy)");
        Option numMaps = new Option("m", "mappers", true,
            "The number of mappers to run (The number of paraller copiers)");
        Option dst = new Option("d", "destUri", true,
            "Destination URI. Must be an absolute path. Default is /user/<username>/backup. Example "
            + "hdfs://example.com:2020/foo/bar or /foo/bar");
        Option usr = new Option("u", "user", true,
            "The hbase user to use. Default is current user logged in");
        Option tbl = new Option("t", "tables", true,
            "Comma delimited list of tables to backup. Default is to create a backup of all tables");

        initialreplication.setRequired(false);
        finalReplication.setRequired(false);
        tries.setRequired(false);
        numMaps.setRequired(false);
        dst.setRequired(false);
        usr.setRequired(false);
        tbl.setRequired(false);

        options.addOption(initialreplication);
        options.addOption(finalReplication);
        options.addOption(tries);
        options.addOption(numMaps);
        options.addOption(dst);
        options.addOption(usr);
        options.addOption(tbl);

        return options;
    }

    /**
     * Print the available options to the display.
     */
    private static void printOptions() {
        HelpFormatter formatter = new HelpFormatter();
        String header = "Tool to backup an HBase database";
        formatter.printHelp("Backup", header, getOptions(), "", true);
    }
}
