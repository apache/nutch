/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.db;

import java.io.*;
import java.util.*;
import java.util.logging.*;

import org.apache.nutch.io.*;
import org.apache.nutch.pagedb.*;
import org.apache.nutch.linkdb.*;

/********************************************
 * A BucketSet holds many buckets, full of instructions.
 *
 * Once created, it can be given a set of byte arrays.  They
 * will be divided into a handy number of buckets.  The BucketSet
 * will look at a certain byte range within the item to decide 
 * which bucket will get the item.  Ideally, your input data
 * will show an even distribution in the byte-range that you
 * indicate.
 *
 * Once you call close(), the BucketSet cannot be read from
 * or written to.  You must reopen it by allocating a new BucketSet.
 *
 * Once you request a single item via getNextItem(), you cannot
 * again add items to the BucketSet.  You can start reading from
 * the beginning of the BucketSet again by recreating it.
 *
 * If you are completely done with the BucketSet, delete() it.
 *
 * @author Mike Cafarella
 ********************************************/
class BucketSet {
    final static String BUCKET_FILENAME = "bucket";
    final static String CONFIG_FILENAME = "config";
    final static int INTEGER_SIZE = 32;
    
    /**
     * Create a new BucketSet from a data set that is already there.
     * You can start adding to the BucketSet again, as long as
     * you have not yet made a call to getNextItem().
     */
    public static BucketSet loadBuckets(File bucketsDir) throws IOException {
        return new BucketSet(bucketsDir);
    }

    /**
     * Create a brand-new BucketSet, at the given File location, 
     * with the appropriate parameters.  If the file already exists,
     * then this will fail.
     */
    public static BucketSet createBuckets(File bucketsDir, int keyStartByte, int keyBits) throws IOException {
        return new BucketSet(bucketsDir, keyStartByte, keyBits);
    }

    // Persistent values
    File bucketsDir;
    int keyStartByte, keyBits, curBucket;
    boolean insertsAllowed;

    // Transient values
    DataInputStream inStreams[];
    DataOutputStream outStreams[];
    int numBuckets;
    boolean closed;

    /**
     * Load an old BucketSet, at bucketsDir.
     */
    BucketSet(File bucketsDir) throws IOException {
        if (! (bucketsDir.exists() && bucketsDir.isDirectory())) {
            throw new IOException("File " + bucketsDir + " either does not exist or is not a directory");
        }
        this.bucketsDir = bucketsDir;
        loadConfig();

        if (insertsAllowed) {
            this.outStreams = new DataOutputStream[numBuckets];
        } else {
            this.inStreams = new DataInputStream[numBuckets];
        }
        this.closed = false;
    }

    /**
     * Create a new BucketSet at bucketsDir, with the given parameters.
     * The number of buckets is determined by how many bits of the
     *   key you allocate toward bucket-selection.  
     */
    BucketSet(File bucketsDir, int keyStartByte, int keyBits) throws IOException {
        if (keyBits > INTEGER_SIZE) {
            throw new IOException("Parameter keyBits is too large: " + keyBits);
        }

        if (bucketsDir.exists()) {
            throw new IOException("Directory " + bucketsDir + " is already present.");
        } else {
            bucketsDir.mkdir();
        }

        this.bucketsDir = bucketsDir;

        // Persistent values
        this.keyStartByte = keyStartByte;
        this.keyBits = keyBits;
        this.curBucket = 0;
        this.insertsAllowed = true;
        storeConfig();

        // Transient ones
        this.numBuckets = (int) Math.pow(2, keyBits);
        if (insertsAllowed) {
            this.outStreams = new DataOutputStream[numBuckets];
        } else {
            this.inStreams = new DataInputStream[numBuckets];
        }
        this.closed = false;
    }

    /**
     * Close down the BucketSet.  No more reading or writing.
     */
    public void close() throws IOException {
        if (closed) {
            throw new IOException("BucketSet closed");
        }
        for (int i = 0; i < numBuckets; i++) {
            if (insertsAllowed) {
                if (outStreams[i] != null) {
                    outStreams[i].close();
                }
            } else {
                if (inStreams[i] != null) {
                    inStreams[i].close();
                }
            }
            new File(bucketsDir, BUCKET_FILENAME + "." + i).delete();
        }
        new File(bucketsDir, CONFIG_FILENAME).delete();
        bucketsDir.delete();

        inStreams = null;
        outStreams = null;
        closed = true;
    }

    /**
     * Write the given byte array to the BucketSet.
     *
     * Use the array's key region to decide which bucket
     * will get it.
     */
    public void storeItem(byte item[]) throws IOException {
        if (closed) { 
            throw new IOException("BucketSet closed");
        }
        if (! insertsAllowed) {
            throw new IOException("Insert no longer allowed to this BucketSet");
        }

        // Before we can store the item, we first need to 
        // compute which bucket to use
        int bucket = computeBucket(item);
        if (outStreams[bucket] == null) {
            outStreams[bucket] = new DataOutputStream(new FileOutputStream(new File(bucketsDir, BUCKET_FILENAME + "." + bucket)));
        }
        
        outStreams[bucket].writeInt(item.length);
        outStreams[bucket].write(item, 0, item.length);
    }

    /**
     * Return the next item in the current bucket.
     * 
     * If we're at the end of a bucket, jump silently to the next.
     *
     * If we're at the end of all buckets, return null.
     */
    public byte[] getNextItem() throws IOException {
        //
        // Phase 0.  Check to make sure it's OK.
        //

        // Make sure we're not closed
        if (closed) {
            throw new IOException("BucketSet closed");
        }
        // If this is the first call to getNextItem(), we need to prepare!
        if (insertsAllowed) {
            // Close down all outstreams, open instreams
            for (int i = 0; i < outStreams.length; i++) {
                if (outStreams[i] != null) {
                    outStreams[i].close();
                }
                outStreams[i] = null;
            }

            inStreams = new DataInputStream[numBuckets];
            outStreams = null;
            insertsAllowed = false;
            curBucket = 0;
            storeConfig();
        }

        //
        // Phase 1.  Find the right bucket
        //

        // Move through all buckets till we find something to read
        int i = 0, itemLen = -1;
        for (i = curBucket; i < numBuckets; i++) {
            // First, open stream if necessary
            if (inStreams[i] == null) {
                File bucketFile = new File(bucketsDir, BUCKET_FILENAME + "." + i);
                if (! bucketFile.exists()) {
                    continue;
                }
                inStreams[i] = new DataInputStream(new FileInputStream(bucketFile));
            }

            // Second, read from stream how many bytes are in item.
            // If we hit the end of the stream, we continue to the
            // next one.
            if (inStreams[i].available() == 0) {
                inStreams[i].close();
                inStreams[i] = null;
            } else {
                itemLen = inStreams[i].readInt();
                break;
            }
        }

        //
        // Phase 2.  Remember the bucket, and read the next item
        //
        
        // Remember where we stopped
        curBucket = i;

        // Check to see if we found an item, or if we have hit
        // the end of the bucket set.
        if (itemLen >= 0) {
            byte newItem[] = new byte[itemLen];
            inStreams[i].readFully(newItem);
            return newItem;
        }
        
        // We have no more buckets!
        return null;
    }

    /**
     * Compute which bucket to use, given the input data
     * (and configuration params).  Return bucket index.
     */
    int computeBucket(byte item[]) {
        int bucketIndex = 0;

        for (int i = 0; i < keyBits; i++) {
            byte curByte = item[keyStartByte + (i / 8)];
            byte curBit = (byte) (0x01 & (curByte >> (7 - (i % 8))));

            bucketIndex = bucketIndex << 1;
            bucketIndex |= curBit;
        }

        return bucketIndex;
    }

    /**
     * Load the BucketSet config file
     */
    void loadConfig() throws IOException {
        File configFile = new File(bucketsDir, CONFIG_FILENAME);
        DataInputStream dis = new DataInputStream(new FileInputStream(configFile));
        try {
            this.keyStartByte = dis.readInt();
            this.keyBits = dis.readInt();
            this.curBucket = dis.readInt();
            this.insertsAllowed = dis.readBoolean();
        } finally {
            dis.close();
        }
    }

    /**
     * Store values out to the BucketSet config file
     */
    void storeConfig() throws IOException {
        File configFile = new File(bucketsDir, CONFIG_FILENAME);
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(configFile));
        try {
            dos.writeInt(keyStartByte);
            dos.writeInt(keyBits);
            dos.writeInt(curBucket);
            dos.writeBoolean(insertsAllowed);
        } finally {
            dos.close();
        }
    }
}

