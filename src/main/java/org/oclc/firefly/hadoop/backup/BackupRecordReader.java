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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Sequence File record reader for backup utility.
 * Copied from SequenceFileRecordReader, but modified for this utility. There is some code in HRegionInfo
 * that doesn't allow HRegionInfo objects to be reused. So, this class instantiates a new object
 * every time it reads a new pair of key value pairs
 */
public class BackupRecordReader extends RecordReader<Text, HRegionInfo> {
    /** the reader */
    private SequenceFile.Reader in;
    
    /** the start of the split */
    private long start;
    
    /** the end of the split */
    private long end;
    
    /** Indicates whether we are done or not */
    private boolean more = true;
    
    /** the map key */
    private Text key = new Text();
    
    /** The map value */
    private HRegionInfo value = null;
    
    /** The configuration */
    protected Configuration conf;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)split;
        conf = context.getConfiguration();
        Path path = fileSplit.getPath();
        FileSystem fs = path.getFileSystem(conf);
        this.in = new SequenceFile.Reader(fs, path, conf);
        this.end = fileSplit.getStart() + fileSplit.getLength();

        if (fileSplit.getStart() > in.getPosition()) {
            in.sync(fileSplit.getStart());
        }

        this.start = in.getPosition();
        more = start < end;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (more) {
            if (in.next(key)) {
                value = new HRegionInfo();
                in.getCurrentValue(value);
            } else {
                more = false;
                key = null;
                value = null;
            }
        }
        
        return more;
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public HRegionInfo getCurrentValue() {
        return value;
    }

    /**
     * Return the progress within the input split
     * @return 0.0 to 1.0 of the input byte range
     * @throws IOException IO exception
     */
    @Override
    public float getProgress() throws IOException {
        float ret = 0.0f;
        
        if (end != start) {
            ret = Math.min(1.0f, (in.getPosition() - start) / (float)(end - start));
        }
        
        return ret;
    }

    /**
     * Close the underlying reader
     * @throws IOException IO exception
     */
    @Override
    public synchronized void close() throws IOException {
        in.close();
    }

}
