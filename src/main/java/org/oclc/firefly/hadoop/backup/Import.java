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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.Writables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool to import a backup copy back to a functional hbase instance
 * Assumes that hbase and backup copy reside on the same HDFS cluster
 */
public class Import {
    
    /** logger */
    private static final Logger LOG = LoggerFactory.getLogger(Import.class);
    
    /** root table name string */
    private static final String ROOT_TABLE_NAME = Bytes.toString(HConstants.ROOT_TABLE_NAME);
    
    /** meta table name string */
    private static final String META_TABLE_NAME = Bytes.toString(HConstants.META_TABLE_NAME);
    
    /** The path to store copies of backups before importing */
    public static final String IMPORT_TMP_BACKUP_DIR = "/tmp/backup/tmpbaks";
    
    /** the cluster configuration */
    private Configuration conf = null;
    
    /** the file system */
    private FileSystem fs = null;
    
    /** the location of the backup */
    private Path backupDirPath = null;
    
    /** retain the original backup copy */
    private boolean retainOriginal = false;

    /** The number of tables imported */
    private int numTablesImported = 0;

    /** The number of tables that failed to import */
    private int numFailedImports = 0;
    
    /** The start date of this backup */
    private Date startDate = null;

    /** The end date of this backup */
    private Date endDate = null;
    
    /** The tables in this backup */
    private List<String> tableNames = null;
    
    /** the hbase admin to use */
    private HBaseAdmin hadmin = null;
    
    /** the user name to run tasks as */
    private String username = System.getProperty("user.name");

    /**
     * Constructor
     * @param conf The cluster configuration
     * @param backupDirPath The backup directory path
     * @throws IOException Thrown if failed to get file system
     * @throws ParseException Thrown if directory does not contain a valid backup name
     */
    public Import(Configuration conf, Path backupDirPath) throws IOException, ParseException {
        init(conf, backupDirPath, false);
    }
    
    /**
     * Constructor
     * @param conf The cluster configuration
     * @param backupDirPath The backup directory path
     * @param ignoreBadName Ignores the backup directory name. Does not parse start/end date of backup
     * @throws IOException Thrown if failed to get file system
     * @throws ParseException Thrown if directory does not contain a valid backup name
     */
    public Import(Configuration conf, Path backupDirPath, boolean ignoreBadName) throws IOException, ParseException {
        init(conf, backupDirPath, ignoreBadName);
    }
    
    /**
     * Used by constructor
     * @param conf The cluster configuration
     * @param backupDirPath The backup directory path
     * @param ignoreBadName Ignores the backup directory name. Does not parse start/end date of backup
     * @throws IOException Thrown if failed to get file system
     * @throws ParseException Thrown if directory does not contain a valid backup name
     */
    private void init(Configuration conf, Path backupDirPath, boolean ignoreBadName)
        throws IOException, ParseException {
        this.conf = conf;
        this.fs = FileSystem.get(conf);
        this.backupDirPath = backupDirPath;
        this.hadmin = new HBaseAdmin(conf);
        
        if (!fs.exists(backupDirPath)) {
            throw new FileNotFoundException("Backup directory " + backupDirPath + " does not exist");
        }

        String backupDirName = backupDirPath.getName();
        String[] splitDirName = backupDirName.split("-");
        
        if (splitDirName.length == 3) {
            if (splitDirName[0].equals("bak")) {
                try {
                    startDate = BackupUtils.BACKUP_DATE_FORMAT.parse(splitDirName[1]);
                    endDate   = BackupUtils.BACKUP_DATE_FORMAT.parse(splitDirName[2]);
                } catch (Exception e) {
                    startDate = null;
                    endDate = null;
                }
            }
        }

        if (!ignoreBadName && (startDate == null || endDate == null || !startDate.before(endDate))) {
            throw new ParseException("Backup directory does not have a valid name", 0);
        }
        
        tableNames = getTableNamesFromBackup();
    }

    /**
     * Import table entry point
     * @param args Command line arguments
     * @throws Exception If failed to read from file system
     */
    public static void main(String[] args) throws Exception {
        boolean copy = false;
        boolean ignoreBadName = false;
        String inputDir = null;
        String tbl = null;
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
            case 'b':
                ignoreBadName = true;
                break;
            case 'i':
                inputDir = option.getValue();
                break;
            case 'c':
                copy = true;
                break;
            case 't':
                tbl = option.getValue();
                break;
            default:
                throw new IllegalArgumentException("unexpected option " + option);
            }
        }

        String[] tables = null;
        Configuration conf = HBaseConfiguration.create();
        Path backupDirPath = new Path(inputDir);
        
        Import importer = new Import(conf, backupDirPath, ignoreBadName);
        importer.setRetainOriginal(copy);
        
        if (tbl == null) {
            tables = importer.getTableNames();
        } else {
            tables = tbl.split(",");
        }
        
        LOG.info("HBase import tool");
        LOG.info("--------------------------------------------------");
        LOG.info("Backup start time   : " + importer.getStartDate());
        LOG.info("Backup end time     : " + importer.getEndDate());
        LOG.info("Retain original copy: " + importer.getRetainOriginal());
        LOG.info("HBase location      : " + conf.get(HConstants.HBASE_DIR));
        LOG.info("Backup location     : " + backupDirPath);
        LOG.info("--------------------------------------------------");
        
        importer.importAll(tables);
        int totalSuccess = importer.getNumTablesImported();
        int totalFailed  = importer.getNumFailedImports();

        LOG.info("Import results");
        LOG.info("--------------------------------------------------");
        LOG.info("Number of tables: " + tables.length);
        LOG.info("Imported tables : " + totalSuccess);
        LOG.info("Failed          : " + totalFailed);
        LOG.info("--------------------------------------------------");
        
        if (totalFailed == 0) {
            LOG.info("Import completed successfully.");
        } else if (totalSuccess > 0) {
            LOG.warn("Import completed but with errors. Please inspect manually.");
        } else {
            LOG.error("Import failed. Please inspect manually.");
            System.exit(1);
        }

        System.exit(0);
    }

    /**
     * Get the list of names of the tables available in this backup.
     * Ignores any directories which are not valid tables
     * @return The list of table names in this directory
     * @throws IOException Thrown if failed to communicate with file system
     */
    private ArrayList<String> getTableNamesFromBackup() throws IOException {
        ArrayList<String> tableNames = new ArrayList<String>();
        FileStatus[] files = fs.listStatus(backupDirPath);

        for (int i = 0; i < files.length; i++) {
            String tableName = files[i].getPath().getName();
            Path tableDirPath = new Path(backupDirPath, tableName);
            if (isValidTable(tableDirPath)) {
                LOG.debug("Found table: " + tableName);
                tableNames.add(tableName);
            } else {
                LOG.debug("Found directory, but not table: " + tableName + " (discarded)");
            }
        }

        return tableNames;
    }
    
    /**
     * Import the given tables from the given backup directory
     * @param tables An array of table names
     * @throws TableNotFoundException If a table is not found in backup copy
     * @throws TableExistsException If a table already exists
     * @throws IOException If failed to read from file system
     */
    public void importAll(String[] tables) throws TableNotFoundException, TableExistsException, IOException {
        numFailedImports = 0;
        numTablesImported = 0;
        
        doChecks(tables);
        
        // make a copy of backup
        if (retainOriginal) {
            LOG.info("Making copy of tables as requested. This may take a while...");

            Path tmpPath = new Path(getTmpBackupDirectory() + "/" + backupDirPath.getName());
            fs.delete(tmpPath, true);
            
            // Only copy the tables that are being imported
            for (String tableName : tables) {
                Path tmpTablePath = new Path(tmpPath + "/" + tableName);
                Path bakupDirTablePath = new Path(backupDirPath + "/" + tableName);
                
                LOG.info(". Copying " + bakupDirTablePath + " to " + tmpTablePath);
                FileUtil.copy(fs, bakupDirTablePath, fs, tmpTablePath, false, true, conf);
            }
            
            backupDirPath = tmpPath;
        }
        
        // Import one table at a time
        LOG.info("Importing tables");
        for (String tableName : tables) {
            LOG.info(". " + tableName);
            
            boolean imported = importTable(backupDirPath, tableName);
            if (!imported) {
                LOG.error("Table not imported");
                numFailedImports++;
            } else {
                numTablesImported++;
            }
        }
    }
    
    /**
     * Performs pre checks before importing any tables
     * @param tables The tables to check
     * @throws TableNotFoundException If a table is not found in backup copy
     * @throws TableExistsException If a table already exists
     * @throws IOException If failed to read from file system
     */
    private void doChecks(String[] tables) throws TableNotFoundException, TableExistsException, IOException {
        for (String tableName : tables) {
            // Cannot overwrite an existing table. Let user deal with it.
            if (tableExists(tableName)) {
                LOG.error(". " + tableName + ": Table already exists.");
                throw new TableExistsException(tableName + ": Table already exists.");
            }

            // Table does not exist in backup copy
            if (!tableNames.contains(tableName)) {
                LOG.error("Backup copy of table '" + tableName + "' not found");
                throw new TableNotFoundException("Backup copy of table '" + tableName + "' not found");
            }
        }
    }

    /**
     * Do import/restore of table
     * @param backupDirPath The path to the backup directory
     * @param tableName The name of the table to import
     * @return True iff import was successful.
     * @throws IOException If failed to read from file system
     */
    public boolean importTable(Path backupDirPath, String tableName) throws IOException {
        boolean ret = false;
        Path hbaseDirPath  = new Path(conf.get(HConstants.HBASE_DIR));
        Path hbaseTableDirPath  = new Path(hbaseDirPath + "/" + tableName);
        Path backupTableDirPath = new Path(backupDirPath + "/" + tableName);

        // Move backup table to hbase
        fs.rename(backupTableDirPath, hbaseTableDirPath);

        LOG.debug("Moved " + backupTableDirPath + " to " + hbaseTableDirPath);
        HTableDescriptor htd = FSTableDescriptors.getTableDescriptor(fs, hbaseTableDirPath);

        if (htd != null) {
            ret = addTableToMeta(hbaseTableDirPath);
        } else {
            LOG.error("Could not get HTableDescriptor from imported table (" + hbaseTableDirPath + ")");
        }
        
        if (!ret) {
            // revert changes
            fs.rename(hbaseTableDirPath, backupTableDirPath);
        }
        
        return ret;
    }

    /**
     * Add table regions to meta table
     * @param tablePath the path to table directory
     * @return True iff successfully added table regions to meta
     * @throws IOException when failed to read from file system
     */
    protected boolean addTableToMeta(Path tablePath) throws IOException {
        boolean ret = true;
        HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
        FileStatus[] files = fs.listStatus(tablePath);
        List<FileStatus> regions = new ArrayList<FileStatus>();
        List<HRegionInfo> regionInfoList = new ArrayList<HRegionInfo>();
        
        // Find region files
        for (FileStatus file : files) {
            String regionName = file.getPath().getName();
            
            if (regionName.equals(HConstants.HREGION_COMPACTIONDIR_NAME)
                || !file.isDir() || regionName.startsWith(".")) {
                continue;
            } else {
                regions.add(file);
            }
        }
        
        //  Get all regions
        for (FileStatus file : regions) {
            Path regionInfoPath = new Path(file.getPath(), HRegion.REGIONINFO_FILE);
            
            if (!fs.exists(regionInfoPath)) {
                LOG.error("Missing .regioninfo: " + regionInfoPath);
                ret = false;
                break;
            } else {
                // get region info file from region directory
                LOG.debug("regioninfo: " + regionInfoPath);
                
                try {
                    FSDataInputStream regionInfoIn = fs.open(regionInfoPath);
                    HRegionInfo hRegionInfo = new HRegionInfo();
                    hRegionInfo.readFields(regionInfoIn);
                    
                    // Regions are set offline when they are split, but still contain data until a compaction
                    // If we successfully copied this region's data, then we try enabling it.
                    if (hRegionInfo.isOffline()) {
                        LOG.warn("Offline region: " + hRegionInfo);
                        LOG.warn("Set offline to false");
                        hRegionInfo.setOffline(false);
                    }
                    
                    // In backup, if a region is split, then the data is copied from the parent region
                    if (hRegionInfo.isSplit()) {
                        LOG.warn("Split region: " + hRegionInfo);
                        LOG.warn("Set split to false");
                        hRegionInfo.setSplit(false);
                    }
                    
                    regionInfoIn.close();
                    regionInfoList.add(hRegionInfo);
                } catch (Exception e) {
                    LOG.error("HRegionInfo could not be read successfully: " + regionInfoPath);
                    ret = false;
                    break;
                }
            }
        }
        
        if (ret) {
            // If everything checks, add regions to meta
            for (HRegionInfo hRegionInfo : regionInfoList) {
                LOG.debug("Importing region: " + hRegionInfo);
                
                Put p = new Put(hRegionInfo.getRegionName());
                p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER, Writables.getBytes(hRegionInfo));
                meta.put(p);
                hadmin.assign(hRegionInfo.getRegionName());
            }
        }
        
        return ret;
    }

    /**
     * Checks that the table does not already exist in hbase
     * @param tableName The table name
     * @return True if the directory is a valid copy of a table. False otherwise
     * @throws IOException IO exception 
     */
    public boolean tableExists(String tableName) throws IOException {
        Path tableDirPath = new Path(conf.get(HConstants.HBASE_DIR), tableName);
        return fs.exists(tableDirPath);
    }
    
    /**
     * Checks if .tableinfo exists for given table. Returns false for ROOT and META tables
     * @param tablePath the table hdfs directory
     * @return true if exists
     * @throws IOException If failed to read from file system
     */
    public boolean isValidTable(Path tablePath) throws IOException {
        boolean ret = false;
        String tableName = tablePath.getName();

        if (!tableName.equals(ROOT_TABLE_NAME) && !tableName.equals(META_TABLE_NAME)) {
            FileStatus status = BackupUtils.getTableInfoPath(fs, tablePath);
            
            if (status != null) {
                ret = fs.exists(status.getPath());
            }
        }
        
        return ret;
    }
    
    /**
     * Get the name of user to run as
     * @return the username
     */
    public String getUsername() {
        return username;
    }
    
    /**
     * Get the temporary directory where to store a copy of a backup before doing the import
     * @return The temporary directory
     */
    public String getTmpBackupDirectory() {
        String ret = IMPORT_TMP_BACKUP_DIR;
        String user = getUsername();
        
        if (user != null) {
            ret = "/user/" + user + ret;
        }
        
        return ret;
    }
    
    /**
     * Get the number of tables that were succesfully imported
     * @return the numTablesImported
     */
    public int getNumTablesImported() {
        return numTablesImported;
    }

    /**
     * Get the number of tables that failed to be imported
     * @return the numFailedImports
     */
    public int getNumFailedImports() {
        return numFailedImports;
    }
    
    /**
     * Get the start date of backup
     * @return the startDate
     */
    public Date getStartDate() {
        return startDate;
    }

    /**
     * Get the end date of backup
     * @return the endDate
     */
    public Date getEndDate() {
        return endDate;
    }
    
    /**
     * Get whether to retain the original copy or not
     * @return true if we keep the orignal, or false otherwise
     */
    public boolean getRetainOriginal() {
        return this.retainOriginal;
    }
    
    /**
     * Set whether to retain the original copy or not
     * @param retainOriginal the retainOriginal to set
     */
    public void setRetainOriginal(boolean retainOriginal) {
        this.retainOriginal = retainOriginal;
    }
    
    /**
     * Get the table names available in this backup
     * @return The table names
     */
    public String[] getTableNames() {
        return this.tableNames.toArray(new String[0]);
    }
    
    /**
     * Returns the command-line options supported.
     * @return the command-line options
     */
    private static Options getOptions() {
        Options options = new Options();

        Option ignore = new Option("b", "ignoreBadName", false,
            "Ignore error if backup directory does not have a valid backup name \"bak-<startTime>-<endTime>\"");
        Option copy = new Option("c", "copy", false,
            "Copy backup files into HBase. Default behavior is to move files because it's faster");
        Option input = new Option("i", "inputDir", true,
            "Path to the backup directory");
        Option tables = new Option("t", "tables", true,
            "The tables to import from backup directory. Default is to import all tables found in backup.");

        ignore.setRequired(false);
        copy.setRequired(false);
        input.setRequired(true);
        tables.setRequired(false);
        
        options.addOption(ignore);
        options.addOption(copy);
        options.addOption(input);
        options.addOption(tables);

        return options;
    }

    /**
     * Print the available options to the display.
     */
    private static void printOptions() {
        HelpFormatter formatter = new HelpFormatter();
        String header = "Tool to import a previous backup into a live HDFS cluster";
        formatter.printHelp("Import", header, getOptions(), "", true);
    }
}
