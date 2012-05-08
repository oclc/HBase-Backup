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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains common methods used by Backup and Import tools
 */
public final class BackupUtils {
    
    /** logger */
    public static final Logger LOG = LoggerFactory.getLogger(BackupUtils.class);
    
    /** 1MB divided by 64KB */
    public static final int ONE_MB_OVER_64KB = 16;
    
    /** date format used in backup directories */
    public static final DateFormat BACKUP_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd.kkmmss.SSS");
    
    /** Pretty print date format */
    public static final DateFormat PRETTY_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss Z");
    
    /** date format used for log copier */
    public static final DateFormat LOG_COPIER_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    
    /**
     * Cannot instantiate
     */
    private BackupUtils() {
        // nothing to do here
    }
    
    /**
     * Looks under the table directory in the filesystem for files with a '.tableinfo' prefix. Returns
     * reference to the 'latest' instance.
     * 
     * @param fs The filesytem where to look
     * @param tableDirPath the hdfs table directory
     * @return The 'current' tableinfo file.
     * @throws IOException If failed to read from file system
     */
    public static FileStatus getTableInfoPath(final FileSystem fs, final Path tableDirPath) throws IOException {
        FileStatus ret = null;
        FileStatus[] status = FSUtils.listStatus(fs, tableDirPath, new PathFilter() {
            @Override
            public boolean accept(Path p) {
                // Accept any file that starts with TABLEINFO_NAME
                return p.getName().startsWith(FSTableDescriptors.TABLEINFO_NAME);
            }
        });

        if (status != null && status.length > 0) {
            Arrays.sort(status, new Comparator<FileStatus>() {
                @Override
                public int compare(FileStatus left, FileStatus right) {
                    return -left.compareTo(right);
                }
            });
            
            if (status.length > 1) {
                // Clean away old versions of .tableinfo
                for (int i = 1; i < status.length; i++) {
                    Path p = status[i].getPath();
                    
                    // Clean up old versions
                    if (!fs.delete(p, false)) {
                        LOG.warn("Failed cleanup of " + status);
                    } else {
                        LOG.debug("Cleaned up old tableinfo file " + p);
                    }
                }
            }
            
            ret = status[0];
        }

        return ret;
    }
    
    /**
     * Return the trash path of a deleted file
     * Borrowed this nice function from https://issues.apache.org/jira/browse/HBASE-5509
     * Although, in the versions of HBase we tested 0.92.* HBase doesn't delete files to trash
     * even when enabled, but leaving it here for now.
     * @param path The original path to the deleted file
     * @param user the hbase user
     * @param fs The filesystem where to look
     * @return The path in trash of the given file, or null if not found.
     * @throws IOException If failed to read from file system
     */
    public static Path getPathInTrash(Path path, String user, FileSystem fs) throws IOException {
        Path ret = null;
        Path trashPath = new Path("/user/" + user + "/.Trash");
        FileStatus[] checkpoints = fs.listStatus(trashPath);
        
        // Go through all checkpoints in .Trash (including Current) to look for our
        // missing file.
        if (checkpoints != null) {
            for (int i = 0; i < checkpoints.length; i++) {
                Path probablePath = new Path(checkpoints[i].getPath().toString() + path.toUri().getPath());
                
                LOG.info("Probable Path : " + probablePath);
                if (fs.exists(probablePath)) {
                    ret = probablePath;
                }
            }
        }
        
        return ret;
    }
    
    /**
     * Flush the given region
     * @param region The HRegionInfo object for the region to flush
     * @param host The region server host
     * @param port The region server port
     * @param conf The dfs configuration object
     * @throws IOException If failed to communicate with the region server
     * @throws NotServingRegionException Thrown if region is not hosted by region server
     */
    public static void flushRegion(HRegionInfo region, String host, int port, Configuration conf)
        throws IOException, NotServingRegionException {
        HConnection conn = HConnectionManager.getConnection(conf);
        HRegionInterface regionServer = conn.getHRegionConnection(host, port);
        regionServer.flushRegion(region);
    }

    /**
     * Get the list of files to copy. 
     * @param fs The file system to get file from
     * @param region The region to get the list of files for
     * @return A list of file names
     * @throws IOException When failed to communicate with filesystem
     */
    public static List<FileStatus> getListOfRegionFiles(FileSystem fs, HRegionInfo region)
        throws IOException {
        List<FileStatus> ret = new ArrayList<FileStatus>();
        String rootDir = fs.getConf().get(HConstants.HBASE_DIR);
        String tableName = region.getTableNameAsString();
        String regionName = region.getEncodedName();
        Path tableDirPath = new Path(rootDir, tableName);

        // Get table descriptor so we may get information about the table we are extracting
        HTableDescriptor tDesc = FSTableDescriptors.getTableDescriptor(fs, tableDirPath);
        
        if (tDesc == null) {
            throw new TableNotFoundException("Could not get HTableDescriptor for table " + tableName);
        }

        // Need to find out what column families this table has
        // so that we may generate paths to the files we are copying
        HColumnDescriptor[] columnFamilies = tDesc.getColumnFamilies();
        
        // Add .regioninfo to list of files to cppy
        Path regionDirPath = new Path(tableDirPath, regionName);
        Path regionInfoFilePath = new Path(regionDirPath, HRegion.REGIONINFO_FILE);
        
        // check that region directory still exists
        if (!fs.exists(regionDirPath)) {
            throw new FileNotFoundException("Region directory no longer exists: " + regionDirPath);
        }

        // need region info file
        FileStatus regionInfoFile = fs.getFileStatus(regionInfoFilePath);
        ret.add(regionInfoFile);

        // Go through each column family directory and list its files
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
                throw e;
            }
        }
        
        return ret;
    }
    
    
    /**
     * Get the relative HBase path of the given path for the given file system
     * @param fs the file system where the path lives
     * @param path the absolute path to hbase file
     * @return Return the relative HBase path
     */
    public static String getFsRelativePath(FileSystem fs, Path path) {
        String root = fs.getConf().get(HConstants.HBASE_DIR);
        return StringUtils.substringAfter(path.toString(), root);
    }
    
    /**
     * Copy the file at inputPath to the destination cluster using the same directory structure
     * @param srcFs The source file system
     * @param dstFs The destination file system
     * @param srcPath The source path
     * @param dstPath The destination path
     * @param buffer The buffer to use
     * @param username The user name. Used when checking in .Trash for deleted files
     * @param replication The replication factor
     * @throws InterruptedException hdfs exception
     * @throws IOException hdfs exception
     */
    public static void copy(FileSystem srcFs, Path srcPath, FileSystem dstFs, Path dstPath, byte[] buffer,
        String username, short replication) throws InterruptedException, IOException {
        copy(srcFs, srcPath, dstFs, dstPath, buffer, username, replication, null);
    }
    
    /**
     * Copy the file at inputPath to the destination cluster using the same directory structure
     * @param srcFs The source file system
     * @param dstFs The destination file system
     * @param srcPath The source path
     * @param dstPath The destination path
     * @param buffer The buffer to use
     * @param username The user name. Used when checking in .Trash for deleted files
     * @param context The mapper context object
     * @param replication The replication factor
     * @throws InterruptedException hdfs exception
     * @throws IOException hdfs exception
     */
    public static void copy(FileSystem srcFs, Path srcPath, FileSystem dstFs, Path dstPath, byte[] buffer,
        String username, short replication, Context context) throws InterruptedException, IOException {
        Path src = srcPath;
        int bytesRead = 0;
        long totalBytesRead = 0;
        
        if (!srcFs.exists(src)) {
            // File no longer exists. Attempt to get from .Trash
            // NOTE: It appears that deleted regions are not sent to trash, even with move to trash enabled,
            // so this file will never be found unless this aspect of hbase changes.
            src = BackupUtils.getPathInTrash(src, username, srcFs);
            
            if (src == null) {
                throw new FileNotFoundException("Could not recover deleted file from trash: " + srcPath);
            }
            
            LOG.warn("File has been deleted, but found it in trash: " + src);
        }

        long bytesToCopy = srcFs.getFileStatus(src).getLen();
        FSDataOutputStream out = dstFs.create(dstPath, replication);
        FSDataInputStream in = srcFs.open(src);
        
        int numWrites = 0;
        while ((bytesRead = in.read(buffer)) >= 0) {
            out.write(buffer, 0, bytesRead);
            totalBytesRead += bytesRead;
            
            if (context != null && numWrites % ONE_MB_OVER_64KB == 0) {
                context.setStatus("Copied " + totalBytesRead + " of " + bytesToCopy + " bytes for " + src);
            }
            
            numWrites++;
        }
        
        in.close();
        out.close();
    }
    
    
    /**
     * Get the HDFS path to the given region
     * @param rootDir The root directory of the tables
     * @param region The region
     * @return The HDFS path to this region
     */
    public static Path getRegionPath(String rootDir, HRegionInfo region) {
        String tableName = region.getTableNameAsString();
        String regionName = region.getEncodedName();
        Path tableDirPath = new Path(rootDir, tableName);
        Path regionDirPath = new Path(tableDirPath, regionName);
        return regionDirPath;
    }
    
    /**
     * Convert to HRegionInfo object
     * @param data The bytes representing the HRegionInfo object
     * @return The HRegionInfo object
     * @throws IOException If data is not a valid HRegionInfo object
     */
    public static HRegionInfo getHRegionInfo(byte[] data) throws IOException {
        HRegionInfo info = null;

        if (data != null) {
            info = Writables.getHRegionInfo(data);
            LOG.debug("HRegionInfo: " + info);
        }

        return info;
    }
}
