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

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;

/*********************************************************
 * The EditSectionGroupWriter maintains a set of EditSectionWriter
 * objects.  It chooses the appropriate EditSectionWriter to
 * carry out each operation.  
 *
 * @author Mike Cafarella
 *********************************************************/
public class EditSectionGroupWriter {
    final static int CUR_VERSION = 0;

    // File for SectionGroupWriter meta-info
    public final static String GROUP_METAINFO = "group_metainfo";
    
    // 
    // Keyspace identifiers
    //
    public static int URL_KEYSPACE = 0;
    public static int MD5_KEYSPACE = 1;

    /*********************************************************
     * Edit instructions are Comparable, but they also have
     * an "inner" key like MD5Hash or URL that is also Comparable.
     * This class extracts that inner key, which we need for 
     * allocating a Page or Link Instruction into the correct
     * bucket.
     *********************************************************/
    public static abstract class KeyExtractor {
        /**
         */
        public KeyExtractor() {
        }

        /**
         */
        public abstract WritableComparable extractInnerKey(WritableComparable key);
    }

    /**
     * Get the URL from a PageInstruction
     */
    public static class PageURLExtractor extends KeyExtractor {
        public PageURLExtractor() {
        }
        public WritableComparable extractInnerKey(WritableComparable key) {
            return ((DistributedWebDBWriter.PageInstruction) key).getPage().getURL();
        }
    }

    /**
     * Get the MD5 from a PageInstruction
     */
    public static class PageMD5Extractor extends KeyExtractor {
        public PageMD5Extractor() {
        }
        public WritableComparable extractInnerKey(WritableComparable key) {
            return ((DistributedWebDBWriter.PageInstruction) key).getPage().getMD5();
        }
    }

    /**
     * Get the URL from a LinkInstruction
     */
    public static class LinkURLExtractor extends KeyExtractor {
        public LinkURLExtractor() {
        }
        public WritableComparable extractInnerKey(WritableComparable key) {
            return ((DistributedWebDBWriter.LinkInstruction) key).getLink().getURL();
        }
    }

    /**
     * Get the MD5 from a LinkInstruction
     */
    public static class LinkMD5Extractor extends KeyExtractor {
        public LinkMD5Extractor() {
        }
        public WritableComparable extractInnerKey(WritableComparable key) {
            return ((DistributedWebDBWriter.LinkInstruction) key).getLink().getFromID();
        }
    }

    /**
     * Initialize an EditSectionGroup.  Tell it the label, the
     * keytype, and the division between keys.
     */
    public static void createEditGroup(NutchFileSystem nfs, File dbDir, String label, int numSections, int keySpaceType) throws IOException {
        // Max num-sections
        if (numSections > DBKeyDivision.MAX_SECTIONS) {
            throw new IllegalArgumentException("Maximum number of sections is " + DBKeyDivision.MAX_SECTIONS);
        }

        // Test for known keyspace type.
        if ((keySpaceType != URL_KEYSPACE) && 
            (keySpaceType != MD5_KEYSPACE)) {
            throw new IllegalArgumentException("Unknown keyspace type: " + keySpaceType);
        }

        File metaInfoDir = new File(new File(dbDir, "standard"), GROUP_METAINFO);
        File metaInfo = new File(metaInfoDir, label);
        DataOutputStream out = new DataOutputStream(nfs.create(metaInfo));
        try {
            out.write(CUR_VERSION);
            out.writeInt(keySpaceType);

            double stepSize = DBKeyDivision.MAX_SECTIONS / (1.0 * numSections);
            if (keySpaceType == URL_KEYSPACE) {
                UTF8 url = new UTF8();
                for (int i = 0; i < numSections; i++) {
                    url.set(DBKeyDivision.URL_KEYSPACE_DIVIDERS[(int) Math.round(i * stepSize)]);
                    url.write(out);
                }
            } else {
                for (int i = 0; i < numSections; i++) {
                    DBKeyDivision.MD5_KEYSPACE_DIVIDERS[(int) Math.round(i * stepSize)].write(out);
                }
            }
        } finally {
            out.close();
        }
    }


    int machineNum = -1, totalMachines = 1;
    KeyExtractor extractor;
    String label;
    WritableComparable sectionKeys[];
    EditSectionWriter sectionWriters[];

    /**
     * Start a EditSectionGroupWriter at the indicated location, for
     * a single emitter.  There will be as many of these as there
     * are processor-machines.  (The emitter value will be different
     * for each.)  The Group must already have been created via a 
     * call to EditSectionGroupWriter.createEditSectionGroupWriter().
     *
     * The EditSectionGroupWriter consists of a bunch of 
     * EditSectionWriters, each of which hold a file we append to.
     */
    public EditSectionGroupWriter(NutchFileSystem nfs, int machineNum, int totalMachines, String label, Class keyClass, Class valClass, EditSectionGroupWriter.KeyExtractor extractor) throws IOException {
        this.machineNum = machineNum;
        this.totalMachines = totalMachines;
        this.extractor = extractor;

        // Bail if the emitter/section numbering is incorrect
        if (machineNum < 0 || machineNum >= totalMachines) {
            throw new IllegalArgumentException("machineNum is " + machineNum + ", and totalMachines is " + totalMachines);
        }

        // Load in details about keys
        File metaInfoDir = new File("standard", GROUP_METAINFO);
        File metaInfo = new File(metaInfoDir, label);
        DataInputStream in = new DataInputStream(nfs.open(metaInfo));
        try {
            int version = in.read();
            int keySpaceType = in.readInt();

            this.sectionKeys = new WritableComparable[totalMachines];
            for (int i = 0; i < sectionKeys.length; i++) {
                WritableComparable key = null;
                if (keySpaceType == URL_KEYSPACE) {
                    key = new UTF8();
                } else {
                    key = new MD5Hash();
                }
                key.readFields(in);
                this.sectionKeys[i] = key;
            }
        } finally {
            in.close();
        }

        // Build all the sections
        this.sectionWriters = new EditSectionWriter[totalMachines];
        for (int i = 0; i < sectionWriters.length; i++) {
            this.sectionWriters[i] = new EditSectionWriter(nfs, label, i, machineNum, keyClass, valClass);
        }
        this.label = label;
    }

    /**
     * Add an instruction and append it.  We need to find an
     * appropriate EditSectionWriter.
     */
    public void append(WritableComparable key, Writable val) throws IOException {
        WritableComparable innerKey = extractor.extractInnerKey(key);

        // If there is noplace to write the item, return
        if (sectionWriters.length == 0) {
            return;
        }

        // (start, end] is the range
        int start = 0, end = sectionWriters.length, pivot = 0;

        while (end - start > 1) {
            pivot = (end + start) / 2;
            int comparison = innerKey.compareTo(sectionKeys[pivot]);

            if (comparison < 0) {
                end = pivot;
            } else if (comparison >= 0) {
                start = pivot;
            }
        }
        sectionWriters[start].append(key, val);
    }

    /**
     * Close down the writers
     */
    public void close() throws IOException {
        for (int i = 0; i < sectionWriters.length; i++) {
            sectionWriters[i].close();
        }
    }
}

