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
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;

/*********************************************************
 * The EditSectionGroupReader will read in an edits-file that
 * was built in a distributed way.  It acts as a "meta-SequenceFile",
 * incorporating knowledge of Section numbering as well as
 * process-synchronization.  If you had different ideas
 * about how to make the db-edits distributed (apart from using
 * NFS), you'd implement them here.
 *
 * @author Mike Cafarella
 *********************************************************/
public class EditSectionGroupReader {
    static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.db.EditSectionGroupReader");
    private final static String MERGED_EDITS = "merged_edits";
    private final static int SLEEP_INTERVAL = 3000;
    private final static int WORRY_INTERVALS = 5;

    NutchFileSystem nfs;
    String label;
    int readerNum = -1, totalMachines = -1, numEdits = 0;
    boolean sectionComplete = false;

    /**
     * Open the EditSectionGroupReader for the appropriate file.
     */
    public EditSectionGroupReader(NutchFileSystem nfs, String label, int readerNum, int totalMachines) {
        this.nfs = nfs;
        this.label = label;
        this.readerNum = readerNum;
        this.totalMachines = totalMachines;
    }

    /**
     * Block until all contributions to the EditSection are present
     * and complete.  To figure out how many contributors there are,
     * we load the meta-info first (which is written at section-create
     * time).
     */
    private synchronized void sectionComplete() throws IOException { 
        if (! sectionComplete) {
            //
            // Make sure that every contributor's file is present.
            // When all are present, we know this section is complete.
            //
            for (int i = 0; i < totalMachines; i++) {
                // Create the files we're interested in
                File allEditsDir = new File("editsection." + readerNum, "editsdir." + i);
                File editsDir = new File(allEditsDir, label);
                File editsList = new File(editsDir, "editslist");
                File editsInfo = new File(editsDir, "editsinfo");

                // Block until the editsInfo file appears
                while (! nfs.exists(editsInfo)) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ie) {
                    }
                }

                // Read in edit-list info
                DataInputStream in = new DataInputStream(nfs.open(editsInfo));
                try {
                    in.read();                 // version
                    this.numEdits += in.readInt();  // numEdits
                } finally {
                    in.close();
                }
            }
            sectionComplete = true;
        }
    }


    /**
     * Return how many edits there are in this section.  This
     * method requires total section-completion before executing.
     */
    public int numEdits() throws IOException {
        sectionComplete();
        return numEdits;
    }

    /**
     * Merge all the components of the Section into a single file
     * and return the location.  This method requires total section-
     * completion before executing.
     */
    public void mergeSectionComponents(File mergedEditsFile) throws IOException {
        // Wait till all edit-contributors are done.
        sectionComplete();

        //
        // Figure out the keyclass
        //
        File allEdits0 = new File("editsection." + readerNum, "editsdir." + 0);
        File editsDir0 = new File(allEdits0, label);
        File editsList0 = new File(editsDir0, "editslist");
        while (! nfs.exists(editsList0)) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ie) {
            }
        }

        SequenceFile.Reader test = new SequenceFile.Reader(nfs, editsList0.getPath());
        Class keyClass = null;
        try {
            keyClass = test.getKeyClass();
        } finally {
            test.close();
        }

        //
        // Now write out contents of each contributor's file
        //
        try {
            Writable key = (Writable) keyClass.newInstance();
            SequenceFile.Writer out = new SequenceFile.Writer(nfs, mergedEditsFile.getPath(), keyClass, NullWritable.class);

            try {
                for (int i = 0; i < totalMachines; i++) {
                    File allEditsDir = new File("editsection." + readerNum, "editsdir." + i);
                    File editsDir = new File(allEditsDir, label);
                    File editsList = new File(editsDir, "editslist");
                    while (! nfs.exists(editsList)) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ie) {
                        }
                    }

                    SequenceFile.Reader in = new SequenceFile.Reader(nfs, editsList.getPath());
                    try {
                        while (in.next(key)) {
                            out.append(key, NullWritable.get());
                        }
                    } finally {
                        in.close();
                    }
                }
            } finally {
                out.close();
            }
        } catch (InstantiationException ie) {
            throw new IOException("Could not create instance of " + keyClass);
        } catch (IllegalAccessException iae) {
            throw new IOException("Could not create instance of " + keyClass);
        }
    }

    /**
     * Get rid of the edits encapsulated by this file.
     */
    public void delete() throws IOException {
        for (int i = 0; i < totalMachines; i++) {
            // Delete the files we're interested in
            File editsDir = new File("editsection." + readerNum, "editsdir." + i);
            File consumedEdits = new File(editsDir, label);
            nfs.delete(consumedEdits);
        }
    }
}
