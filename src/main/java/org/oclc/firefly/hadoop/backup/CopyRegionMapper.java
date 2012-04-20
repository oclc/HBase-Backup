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
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies regions to the destination filesystem
 * The key/value input pairs are regionserver/HRegionInfo
 * For every region, it flushes the region, then it gets a list of files for that region
 * It then copies every file that we got for that region.
 * If the region fails. The entire region is failed and the copied files are deleted.
 * Failed regions are added to the retry file which are later examined by the backup utility.
 */
public class CopyRegionMapper extends Mapper<Text, HRegionInfo, Text, HRegionInfo> {
    /** Class Logger */
    private static final Logger LOG = LoggerFactory.getLogger(CopyRegionMapper.class);
    
    /** default buffer size */
    private static final int BUFFER_SIZE = 65536;
    
    /** the replication factor */
    private short replication;
    
    /** the source file system */
    private FileSystem srcFs;

    /** the destination file system */
    private FileSystem dstFs;
    
    /** the user name to use */
    private String username;
    
    /** backup destination path */
    private Path dstPath;
    
    /** the retry file SequenceFile writer */
    private SequenceFile.Writer writer = null;
    
    /** the internal buffer */
    private byte[] buffer = new byte[BUFFER_SIZE];
    
    /** The source configuration */
    private Configuration srcConf = null;
    
    /** The destination configuration */
    private Configuration dstConf = null;
    
    /** The attempt id */
    private int attemptId;

    
    /**
     * Setup the mapper
     * @param context The mapper context object
     * @throws IOException If failed to reach source or destination filesystems
     */
    @Override
    public void setup(Context context) throws IOException {
        srcConf = context.getConfiguration();
        dstConf = new Configuration();
        
        String dst = srcConf.get(Backup.JOBCONF_DEST_HDFS);
        username = srcConf.get(Backup.JOBCONF_USER);
        dstPath = new Path(srcConf.get(Backup.JOBCONF_DEST_PATH));
        replication = (short)srcConf.getInt(Backup.JOBCONF_REPLICATION, 1);
        attemptId = srcConf.getInt(Backup.JOBCONF_ATTEMPT_ID, 0);
                
        if (dst != null) {
            dstConf.set("fs.default.name", dst);
        }

        srcFs = FileSystem.get(srcConf);
        dstFs = FileSystem.get(dstConf);
        
        if (srcFs == dstFs) {
            LOG.info("Source and destination file systems are the same");
        }
        
        LOG.info("Attempt id: " + attemptId);
    }
    
    /**
     * Clean up mapper
     * @param context the mapper context object
     */
    @Override
    public void cleanup(Context context) {
        // Close retry file if open
        if (writer != null) {
            try {
                writer.sync();
                writer.close();
            } catch (IOException e) {
                LOG.warn("Failed to close retry file");
            }
        }
        
        // Only close destination fs. Map reduce framework will close source fs
        if (srcFs != dstFs) {
            try {
                dstFs.close();
            } catch (IOException e) {
                LOG.warn("Failed to close destination file system");
            }
        }
    }
    
    /**
     * Flushes the given region and then gets a list of files belonging to a region.
     * Then it copies the files to their destination directory
     * @param key The region server
     * @param region The HRegionInfo object
     * @param context The mapper context
     * @throws InterruptedException exception
     * @throws IOException exception
     */
    @Override
    public void map(Text key, HRegionInfo region, Context context)
        throws InterruptedException, IOException {
        long total = 0;
        Path relPath = null;
        Path srcFilePath = null;
        Path dstFilePath = null;
        Path regionPath = BackupUtils.getRegionPath(dstPath.toString(), region);

        try {
            String regionServer = key.toString();
            String host = StringUtils.substringBeforeLast(regionServer, ":");
            int    port = Integer.parseInt(StringUtils.substringAfterLast(regionServer, ":"));

            LOG.info("Flushing region " + region + " at " + regionServer);
            BackupUtils.flushRegion(region, host, port, context.getConfiguration());
            
            // delete copy of region if already exists. This would happen
            // if an exception occurred that could not be caught in a previous attempt
            if (dstFs.exists(regionPath)) {
                dstFs.delete(regionPath, true);
            }
            
            // get a list of files to copy
            List<FileStatus> files = BackupUtils.getListOfRegionFiles(srcFs, region);
            
            // copy each
            for (FileStatus file : files) {
                srcFilePath = file.getPath();
                relPath = new Path(BackupUtils.getFsRelativePath(srcFs, srcFilePath));
                dstFilePath = new Path(dstPath.toString() + relPath.toString());
                
                if (file.isDir()) {
                    LOG.info("Creating " + dstFilePath);
                    dstFs.mkdirs(dstFilePath);
                } else {
                    LOG.info("Copying " + srcFilePath + " to " + dstFilePath);
                    BackupUtils.copy(srcFs, srcFilePath, dstFs, dstFilePath, buffer, username, replication, context);
                    
                    total++;
                }
            }

            context.getCounter("Backup", "FilesCopied").increment(total);
            context.getCounter("Backup", "RegionsCopied").increment(1L);
        } catch (FileNotFoundException e) {
            LOG.error("Could not find file", e);
            failRegion(key, region, regionPath, context);
        } catch (NotServingRegionException e) {
            LOG.error("An exception occurred while attempting to flush region", e);
            failRegion(key, region, regionPath, context);
        } catch (Exception e) {
            LOG.error("An exception occurred while attempting to copy file", e);
            failRegion(key, region, regionPath, context);
        }
    }

    /**
     * Fail the region. Add it to retry files
     * @param key The map key
     * @param region the map value
     * @param regionPath The region destination path
     * @param context the mapper context
     * @throws IOException if Failed to read from HDFS
     * @throws InterruptedException if Failed to write output
     */
    private void failRegion(Text key, HRegionInfo region, Path regionPath, Context context)
        throws IOException, InterruptedException {
        context.write(key, region);
        
        LOG.error("Failing entire region. Deleting destination region directory " + regionPath);
        dstFs.delete(regionPath, true);
        
        context.getCounter("Backup", "FailedRegions").increment(1L);
    }
}
