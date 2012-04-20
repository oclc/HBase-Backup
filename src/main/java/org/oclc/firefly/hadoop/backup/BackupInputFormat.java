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
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Input format for back up map reduce
 * @param <Text> 
 * @param <HRegionInfo>
 */
public class BackupInputFormat<Text, HRegionInfo> extends FileInputFormat<Text, HRegionInfo> {

    @SuppressWarnings("unchecked")
    @Override
    public RecordReader<Text, HRegionInfo> createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException {
        return (RecordReader<Text, HRegionInfo>)new BackupRecordReader();
    }

    @Override
    protected long getFormatMinSplitSize() {
        return SequenceFile.SYNC_INTERVAL;
    }

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> files = super.listStatus(job);
        int len = files.size();
        
        for (int i = 0; i < len; ++i) {
            FileStatus file = files.get(i);
            if (file.isDir()) {
                Path p = file.getPath();
                FileSystem fs = p.getFileSystem(job.getConfiguration());
                files.set(i, fs.getFileStatus(new Path(p, MapFile.DATA_FILE_NAME)));
            }
        }
        
        return files;
    }
}
