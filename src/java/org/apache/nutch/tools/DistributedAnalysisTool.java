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

package org.apache.nutch.tools;

import java.io.*;
import java.util.*;
import java.util.logging.*;

import org.apache.nutch.io.*;
import org.apache.nutch.db.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.net.*;
import org.apache.nutch.util.*;
import org.apache.nutch.linkdb.*;
import org.apache.nutch.pagedb.*;
import org.apache.nutch.fetcher.*;

/***************************************
 * DistributedAnalysisTool performs link-analysis by reading
 * exclusively from a IWebDBReader, and writing to
 * an IWebDBWriter.
 *
 * This tool can be used in phases via the command line
 * to compute the LinkAnalysis score across many machines.
 *
 * For a single iteration of LinkAnalysis, you must have:
 *
 * 1) An "initRound" step that writes down how the work should be
 *      divided.  This outputs a "dist" directory which must be made
 *      available to later steps.  It requires the input db directory.
 *
 * 2) As many simultaneous "computeRound" steps as you like, but this
 *      number must be determined in step 1.  Each step may be run
 *      on different machines, or on the same, or however you like.
 *      It requires the the "db" and "dist" directories (or copies) as
 *      inputs.  Each run will output an "instructions file".
 *
 * 3) A "completeRound" step, which integrates the results of all the
 *      many "computeRound" steps.  It writes to a "db" directory.  It
 *      assumes that all the instructions files have been gathered into
 *      a single "dist" input directory.  If you're running everything
 *      on a single filesystem, this will happen easily.  If not, then
 *      you will have to gather the files by hand (or with a script).
 *    
 * For more iterations, repeat steps 1 - 3!
 *
 * @author Mike Cafarella
 ***************************************/
public class DistributedAnalysisTool {   
    final private static String ASSIGN_FILE_PREFIX = "assignment";
    final private static String SCORE_EDITS_FILE_PREFIX = "scoreEdits";
    final private static String ASSIGN_COMPLETE = "assignComplete";
    
    final private static float DEFAULT_SCORE = 0.15f;
    final private static float DECAY_VALUE = 0.85f;

    public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.tools.DistributedAnalysisTool");

    /**
     * The EditSet inner class represents all of the sorted edits
     * files we must process.  The edit-loop can repeatedly ask
     * an EditSet for the "next item", and the EditSet will 
     * seamlessly deal with opening and closing files
     */
    class EditSet {
        File distDir;
        int numEditFiles;
        int curEditFile;
        SequenceFile.Reader curReader;

        /**
         * The "distDir" is where we find all the edit files.
         * The "numEditFiles" is now many we can expect to get there.
         */
        public EditSet(File distDir, int numEditFiles) throws IOException {
            this.distDir = distDir;
            this.numEditFiles = numEditFiles;
            this.curEditFile = 0;
            getNextReader();
        }

        /**
         * Get the next item for reading, closing and opening
         * files if necessary.  Return false if there are no
         * more items to return.
         */
        public synchronized boolean next(Writable key, Writable val) throws IOException {
            //
            // Open the next input stream if necessary
            //
            if (curReader == null) {
                getNextReader();
                // Assume each edits-file has at least one entry in it.
                if (curReader == null) {
                    return false;
                }
            }
            return curReader.next(key, val);
        }

        /**
         * Create the next edit reader and return it.
         */
        private void getNextReader() throws IOException {
            if (curReader != null) {
                curReader.close();
            }

            if (curEditFile < numEditFiles) {
                curReader = new SequenceFile.Reader(nfs, new File(distDir, SCORE_EDITS_FILE_PREFIX + "." + curEditFile + ".sorted").getPath());
                LOG.info("Opened stream to file " + curEditFile);
                curEditFile++;
            }
        }

        /**
         */
        public synchronized void close() throws IOException {
            if (curReader != null) {
                curReader.close();
            }
            curEditFile = numEditFiles;
        }
    }

    /**
     * This is a Writable version of a Float.  We
     * need this so we can store it in a SequenceFile
     */
    class ScoreValue implements Writable {
        float score;
        float nextScore;

        /**
         */
        public ScoreValue() {
        }
        /**
         */
        public void setScore(float f) {
            this.score = f;
        }
        /**
         */
        public void setNextScore(float f) {
            this.nextScore = f;
        }

        /**
         */
        public float score() {
            return score;
        }
        /**
         */
        public float nextScore() {
            return nextScore;
        }

        /**
         */
        public void write(DataOutput out) throws IOException {
            out.writeFloat(score);
            out.writeFloat(nextScore);
        }

        /**
         */
        public void readFields(DataInput in) throws IOException {
            this.score = in.readFloat();
            this.nextScore = in.readFloat();
        }
    }

    NutchFileSystem nfs;
    File dbDir;

    /**
     * Give the pagedb and linkdb files and their cache sizes
     */
    public DistributedAnalysisTool(NutchFileSystem nfs, File dbDir) throws IOException, FileNotFoundException {
        this.nfs = nfs;
        this.dbDir = dbDir;
    }

    /**
     * This method prepares the ground for a set of processes
     * to distribute a round of LinkAnalysis work.  It writes out
     * the "assignments" to a directory.  This directory must be
     * made accessible to all the processes.  (It may be mounted by
     * all of them, or copied to all of them.)
     *
     * This is run by a single process, and it is run first.
     */
    public boolean initRound(int numProcesses, File distDir) throws IOException {
        //
        // The distDir must be empty or non-existent.
        //
        if ((nfs.exists(distDir) && nfs.isFile(distDir)) ||
            (nfs.exists(distDir) && (nfs.listFiles(distDir).length != 0))) {
            LOG.severe("Must be an empty or non-existent dir: " + distDir);
            return false;
        }
        if (! nfs.exists(distDir)) {
            nfs.mkdirs(distDir);
        }

        //
        // Figure out how many db items we have, and how many
        // processes they are allocated to.
        //
        long startPages[] = new long[numProcesses];
        long totalPages = 0;
        IWebDBReader reader = new WebDBReader(nfs, dbDir);
        try {
            totalPages = reader.numPages();     
        } finally {
            reader.close();
        }
        long chunkSize = totalPages / numProcesses;
        long pagesProcessedSoFar = 0;

        //
        // From zero to the 2nd-to-last item, assign a
        // chunk's worth of pages.  The value at each index
        // indicates the start page for that process.
        //
        startPages[0] = 0;
        for (int i = 1; i < numProcesses; i++) {
            startPages[i] = startPages[i-1] + chunkSize;
        }

        //
        // Emit the assignments for the processes
        //
        try {
            // Write out each file
            for (int i = 0; i < numProcesses; i++) {
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(nfs.create(new File(distDir, ASSIGN_FILE_PREFIX + "." + i))));
                try {
                    // Start page
                    out.writeLong(startPages[i]);

                    // How many pages to process
                    if (i != numProcesses - 1) {
                        out.writeLong(chunkSize);
                    } else {
                        // in last index, make up for remainders
                        out.writeLong(totalPages - ((numProcesses - 1) * chunkSize));
                    }
                } finally {
                    out.close();
                }
            }
            
            //
            // Write a file that indicates we finished correctly.
            // This makes it easier for controlling scripts to
            // check whether this process completed.
            //
            // It also holds some overall instruction information,
            // so we can do some error-checking at complete-time.
            //
            File completeFile = new File(distDir, "assignComplete");
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(nfs.create(completeFile)));
            try {
                out.writeInt(numProcesses);
                out.writeLong(totalPages);
                
                // Compute extents
                long extent[] = new long[numProcesses];
                for (int i = 0; i < numProcesses - 1; i++) {
                    extent[i] = chunkSize * (i + 1);
                }
                extent[numProcesses-1] = totalPages - (chunkSize * (numProcesses - 1));
                
                // Emit extents
                for (int i = 0; i < extent.length; i++) {
                    out.writeLong(extent[i]);
                }
            } finally {
                out.close();
            }
            return true;
        } catch (IOException ex) {
            LOG.severe(ex.toString());
            LOG.severe("Sorry, could not finish assignments");
        }
        return false;
    }

    /**
     * This method is invoked by one of the many processes involved
     * in LinkAnalysis.  There will be many of these running at the
     * same time.  That's OK, though, since there's no locking
     * that has to go on between them.
     *
     * This computes the LinkAnalysis score for a given region
     * of the database.  It writes its ID, the region params, and
     * the scores-to-be-written into a flat file.  This file is
     * labelled according to its processid, and is found inside distDir.
     */
    public void computeRound(int processId, File distDir) throws IOException {
        File assignFile = new File(distDir, ASSIGN_FILE_PREFIX + "." + processId);

        long startIndex = 0, extent = 0;
        DataInputStream in = new DataInputStream(new BufferedInputStream(nfs.open(assignFile)));
        try {
            startIndex = in.readLong();
            extent = in.readLong();
        } finally {
            in.close();
        }

        LOG.info("Start at: "+  startIndex);
        LOG.info("Extent: "+  extent);

        // 
        // Open scoreEdits file for this process.  Write down 
        // all the score-edits we want to perform.
        //
        File scoreEdits = new File(distDir, SCORE_EDITS_FILE_PREFIX + "." + processId);
        SequenceFile.Writer scoreWriter = new SequenceFile.Writer(nfs, scoreEdits.getPath() + ".unsorted", UTF8.class, ScoreValue.class);

        //
        // Go through the appropriate WebDB range, and figure out 
        // next scores
        //
        try {
            // Iterate through all items in the webdb, sorted by URL
            long curIndex = 0;
            ScoreValue score = new ScoreValue();
            IWebDBReader reader = new WebDBReader(nfs, dbDir);
            try {
                for (Enumeration e = reader.pagesByMD5(); e.hasMoreElements(); curIndex++) {
                    //
                    // Find our starting place
                    //
                    if (curIndex < startIndex) {
                        e.nextElement();
                        continue;
                    }

                    //
                    // Bail if we've been here too long
                    //
                    if (curIndex - startIndex > extent) {
                        break;
                    }

                    //
                    // OK, do some analysis!
                    //
                    Page curPage = (Page) e.nextElement();
                    Link outLinks[] = reader.getLinks(curPage.getMD5());
                    int targetOutlinkers = 0;
                    for (int i = 0; i < outLinks.length; i++) {
                        if (outLinks[i].targetHasOutlink()) {
                            targetOutlinkers++;
                        }
                    }

                    //
                    // For our purposes here, assume every Page
                    // has an inlink, even though that might not
                    // really be true.  It's close enough.
                    //

                    //
                    // In case there's no previous nextScore, grab
                    // score as an approximation.
                    //
                    float curNextScore = curPage.getNextScore();
                    if (outLinks.length > 0 && curNextScore == 0.0f) {
                        curNextScore = curPage.getScore();
                    }

                    //
                    // Compute contributions
                    //
                    float contributionForAll = (outLinks.length > 0) ? (curNextScore / outLinks.length) : 0.0f;
                    float contributionForOutlinkers = (targetOutlinkers > 0) ? (curNextScore / targetOutlinkers) : 0.0f;
                    for (int i = 0; i < outLinks.length; i++) {
                        // emit the target URL and the contribution
                        score.setScore(contributionForAll);
                        score.setNextScore(outLinks[i].targetHasOutlink() ? contributionForOutlinkers : 0.0f);
                        scoreWriter.append(outLinks[i].getURL(), score);
                    }

                    if (((curIndex - startIndex) % 5000) == 0) {
                        LOG.info("Pages consumed: " + (curIndex - startIndex) + " (at index " + curIndex + ")");
                    }
                }
            } finally {
                reader.close();
            }
        } finally {
            scoreWriter.close();
        }

        // Now sort the resulting score-edits file
        SequenceFile.Sorter sorter = new SequenceFile.Sorter(nfs, new UTF8.Comparator(), ScoreValue.class);
        sorter.sort(scoreEdits.getPath() + ".unsorted", scoreEdits.getPath() + ".sorted");
        nfs.delete(new File(scoreEdits.getPath() + ".unsorted"));
    }


    /**
     * This method collates and executes all the instructions
     * computed by the many executors of computeRound().  It
     * figures out what to write by looking at all the flat
     * files found in the distDir.  These files are labelled
     * according to the processes that filled them.  This method
     * will check to make sure all those files are present
     * before starting work.
     *
     * If the processors are distributed, you might have to
     * copy all the instruction files to a single distDir before
     * starting this method.
     *
     * Of course, this method is executed on only one process.
     * It is run last.
     */
    public void completeRound(File distDir, File scoreFile) throws IOException {
        //
        // Load the overall assignment file, so we can
        // see how many processes we have and how many
        // operations each should include
        //
        int numProcesses = 0;
        long totalPages = 0;
        long extent[] = null;
        File overall = new File(distDir, "assignComplete");
        DataInputStream in = new DataInputStream(new BufferedInputStream(nfs.open(overall)));
        try {
            numProcesses = in.readInt();
            totalPages = in.readLong();
            extent = new long[numProcesses];
            for (int i = 0; i < numProcesses; i++) {
                extent[i] = in.readLong();
            }
        } finally {
            in.close();
            in = null;
        }
        
        //
        // Go through each instructions file we have, and
        // apply each one to the webdb.
        //
        ScoreStats scoreStats = new ScoreStats();
        IWebDBReader reader = new WebDBReader(nfs, dbDir);
        IWebDBWriter writer = new WebDBWriter(nfs, dbDir);
        EditSet editSet = new EditSet(distDir, numProcesses);
        try {
            int count = 0;
            UTF8 curEditURL = new UTF8();
            ScoreValue curContribution = new ScoreValue();
            boolean hasEdit = editSet.next(curEditURL, curContribution);

            //
            // Go through all the Pages, in URL-sort order.
            // We also read from the score-edit file in URL-sort order.
            //
            for (Enumeration e = reader.pages(); e.hasMoreElements(); count++) {
                Page curPage = (Page) e.nextElement();
                if (! hasEdit) {
                    break;
                }

                //
                // Apply the current score-edit to the db item,
                // if appropriate
                //
                int comparison = curPage.getURL().compareTo(curEditURL);
                float newScore = 0.0f, newNextScore = 0.0f;
                if (comparison < 0) {
                    // Fine.  The edit applies to a Page we will
                    // hit later.  Ignore it, and move onto the next
                    // Page.  This should only happen with Pages
                    // that have no incoming links, which are necessarily
                    // special-case Pages.
                    // 
                    // However, that means the Page's score should
                    // be set to the minimum possible, as we have no
                    // incoming links.
                    newScore = (1 - DECAY_VALUE);
                    newNextScore = (1 - DECAY_VALUE);
                } else if (comparison > 0) {
                    // Error!  We should never hit this situation.
                    // It means we have a score-edit for an item
                    // that's not found in the database!
                    throw new IOException("Impossible situation.  There is a score-edit for " + curEditURL + ", which comes after the current Page " + curPage.getURL());
                } else {
                    //
                    // The only really interesting case is when the
                    // score-edit and the curPage are the same.
                    //
                    // Sum all the contributions
                    while (hasEdit && curPage.getURL().compareTo(curEditURL) == 0) {
                        newScore += curContribution.score();
                        newNextScore += curContribution.nextScore();
                        hasEdit = editSet.next(curEditURL, curContribution);
                    }
                    
                    newScore = (1 - DECAY_VALUE) + (DECAY_VALUE * newScore);
                    newNextScore = (1 - DECAY_VALUE) + (DECAY_VALUE * newNextScore);
                }
                
                // Finally, assign it.
                curPage.setScore(newScore, newNextScore);
                writer.addPageWithScore(curPage);
                scoreStats.addScore(newScore);
                if ((count % 5000) == 0) {
                    LOG.info("Pages written: " + count);
                }
            }
            LOG.info("Pages encountered: " + count);
            LOG.info("Target pages from init(): " + totalPages);
        } finally {
            reader.close();
            editSet.close();
            writer.close();
        }

        //
        // Emit the score distribution info
        //
        if (scoreFile.exists()) {
            scoreFile.delete();
        }
        PrintStream pout = new PrintStream(new BufferedOutputStream(nfs.create(scoreFile)));
        try {
            scoreStats.emitDistribution(pout);
        } finally {
            pout.close();
        }

        //
        // Delete all the distributed overhead files
        //
        FileUtil.fullyDelete(nfs, distDir);
    }

    /**
     * Kick off the link analysis.  Submit the locations of the
     * Webdb and the number of iterations.
     *
     * DAT -initRound <n_processes> <dist_dir> <db_dir>
     * DAT -computeRound <process_id> <dist_dir> <db_dir>
     * DAT -completeRound <dist_dir> <db_dir>
     */
    public static void main(String argv[]) throws IOException {
        if (argv.length < 2) {
            System.out.println("usage: java org.apache.nutch.tools.DistributedAnalysisTool (-local | -ndfs <namenode:port>) -initRound|-computeRound|-completeRound (numProcesses | processId) <dist_dir> <db_dir>");
            return;
        }

        String command = null;
        int numProcesses = 0, processId = 0, numIterations = 0;
        File distDir = null, dbDir = null;

        NutchFileSystem nfs = NutchFileSystem.parseArgs(argv, 0);
        for (int i = 0; i < argv.length; i++) {
            if ("-initRound".equals(argv[i])) {
                command = argv[i];
                numProcesses = Integer.parseInt(argv[i+1]);
                distDir = new File(argv[i+2]);
                dbDir = new File(argv[i+3]);
                i+=3;
            } else if ("-computeRound".equals(argv[i])) {
                command = argv[i];
                processId = Integer.parseInt(argv[i+1]);
                distDir = new File(argv[i+2]);
                dbDir = new File(argv[i+3]);
                i+=3;
            } else if ("-completeRound".equals(argv[i])) {
                command = argv[i];
                distDir = new File(argv[i+1]);
                dbDir = new File(argv[i+2]);
                i+=2;
            }
        }

        System.out.println("Started at " + new Date(System.currentTimeMillis()));
        try {
            DistributedAnalysisTool dat = 
                new DistributedAnalysisTool(nfs, dbDir);
            if ("-initRound".equals(command)) {
                dat.initRound(numProcesses, distDir);
            } else if ("-computeRound".equals(command)) {
                dat.computeRound(processId, distDir);
            } else if ("-completeRound".equals(command)) {
                dat.completeRound(distDir, new File(dbDir, "linkstats.txt"));
            } else {
                System.out.println("No directive.");
            }
        } finally {
            System.out.println("Finished at " + new Date(System.currentTimeMillis()));
            nfs.close();
        }
    }
}
