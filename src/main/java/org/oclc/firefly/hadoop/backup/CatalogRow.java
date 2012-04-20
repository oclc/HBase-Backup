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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Class to represent a row from from .META. or -ROOT-
 */
class CatalogRow {
    /** row key */
    private String key;
    
    /** info:regioninfo */
    private byte[] regionInfo;
    
    /** info:server */
    private String server;
    
    /** info:serverstartcode */
    private long serverStartCode;
    
    /** the region server host */
    private String host;
    
    /** the region server port */
    private int port;
    
    /** the HRegionInfo */
    private HRegionInfo hRegionInfo;
    
    /** Split A */
    private HRegionInfo splitA = null;

    /** split B */
    private HRegionInfo splitB = null;
    
    /** default port */
    public static final int DEFAULT_PORT = 80;
    
    /**
     * Construct a new CatalogRow
     * @param r HBase Result
     */
    public CatalogRow(Result r) {
        this.key = new String(r.getRow());
        this.regionInfo = r.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
        this.server = new String(r.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER));
        this.serverStartCode = Bytes.toLong(r.getValue(HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER));

        try {
            hRegionInfo = BackupUtils.getHRegionInfo(this.regionInfo);
        } catch (IOException e1) {
            hRegionInfo = null;
        }
        
        // Get split A HRegionInfo objects
        if (r.containsColumn(HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER)) {
            try {
                byte[] splitData = r.getValue(HConstants.CATALOG_FAMILY, HConstants.SPLITA_QUALIFIER);
                splitA = BackupUtils.getHRegionInfo(splitData);
            } catch (IOException e1) {
                splitA = null;
            }
        }
        
        // Get split B HRegionInfo objects
        if (r.containsColumn(HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER)) {
            try {
                byte[] splitData = r.getValue(HConstants.CATALOG_FAMILY, HConstants.SPLITB_QUALIFIER);
                splitB = BackupUtils.getHRegionInfo(splitData);
            } catch (IOException e1) {
                splitB = null;
            }
        }
        
        String[] split = this.server.split(":");
        this.host = split[0];
        
        try {
            this.port = Integer.parseInt(split[1]);
        } catch (Exception e) {
            this.port = DEFAULT_PORT;
        }
    }
    
    /**
     * Get region server host
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * Get region server port
     * @return the port
     */
    public int getPort() {
        return port;
    }
    
    /**
     * Get the HRegionInfo object
     * @return the HRegionInfo object
     */
    public HRegionInfo getHRegionInfo() {
        return hRegionInfo;
    }
    
    /**
     * Get the HRegionInfo bytes object
     * @return the HRegionInfo bytes
     */
    public byte[] getHRegionInfoBytes() {
        return regionInfo;
    }
    
    
    /**
     * Get split A HRegionInfo object
     * @return the splitA
     */
    public HRegionInfo getSplitA() {
        return splitA;
    }

    /**
     * Get split B HRegionInfo object
     * @return the splitB
     */
    public HRegionInfo getSplitB() {
        return splitB;
    }

    /**
     * Returns true if the start and end key of this row's HRegionInfo object is within the
     * range of the given HRegionInfo start and end key
     * @param parent The region to check against
     * @return True if this catalog row's region is a daughter region of given region. False otherwise 
     */
    public boolean isDaughterOf(HRegionInfo parent) {
        boolean ret = false;

        if (Bytes.compareTo(parent.getTableName(), hRegionInfo.getTableName()) == 0) {
            byte[] rangeStartKey = hRegionInfo.getStartKey();
            byte[] rangeEndKey   = hRegionInfo.getEndKey();
            
            if (rangeStartKey.length > 0 && rangeEndKey.length == 0) {
                // Special case because start and end keys can look the same if they are empty
                // An empty key is naturally less than anything. So if an end key is empty, then it is mistaken
                // as a really small value, rather than a really large value
                ret = (Bytes.compareTo(parent.getEndKey(), rangeEndKey) == 0
                    && Bytes.compareTo(rangeStartKey, parent.getStartKey()) >= 0);
            } else {
                ret = parent.containsRange(rangeStartKey, rangeEndKey);
            }
        }
        
        return ret;
    }
    
    @Override
    public String toString() {
        String ret = "RowKey: " + key + "\n"
            + " regionInfo: " + regionInfo + "\n"
            + " server: " + server + "\n"
            + " serverStartCode: " + serverStartCode;
        return ret;
    }
}
