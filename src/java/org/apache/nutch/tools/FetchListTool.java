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
import java.net.*;
import java.util.*;
import java.text.*;
import java.util.logging.*;

import org.apache.nutch.db.*;
import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;
import org.apache.nutch.pagedb.*;
import org.apache.nutch.linkdb.*;

/**********************************************
 * This class takes an IWebDBReader, computes a relevant subset,
 * and then emits the subset.
 *
 * @author Mike Cafarella
 ***********************************************/
public class FetchListTool {
    public static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.tools.FetchListTool");
    private static String TOP_N_SORTER = "topNSorter";

    private static final long FETCH_GENERATION_DELAY_MS = 7 * 24 * 60 * 60 * 1000;
    private boolean scoreByLinkCount =
      NutchConf.get().getBoolean("fetchlist.score.by.link.count", false);
    
    NutchFileSystem nfs;
    File dbDir;
    boolean refetchOnly;
    float cutoffScore;
    int seed;

    /**
     * The TableSet class will allocate a given FetchListEntry
     * into one of several ArrayFiles.  It chooses which
     * ArrayFile based on a hash of the URL's domain name.
     *
     * It uses a hash of the domain name so that pages are
     * allocated to a random ArrayFile, but same-host pages
     * go to the same file (for efficiency purposes during
     * fetch).
     *
     * Further, within a given file, the FetchListEntry items
     * appear in random order.  This is so that we don't
     * hammer the same site over and over again during fetch.
     *
     * Each table should receive a roughly
     * even number of entries, but all URLs for a  specific 
     * domain name will be found in a single table.  If
     * the dataset is weirdly skewed toward large domains,
     * there may be an uneven distribution.
     */
    class TableSet {
        Vector outputPaths = new Vector();
        Vector tables = new Vector();
        long appendCounts[];
        boolean hasAppended = false;

        /**
         */
        public TableSet() {
        }

        /**
         * Add a table to the list.  Cannot be called
         * after we start appending entries.
         */
        public synchronized boolean add(String outputPath) throws IOException {
            if (hasAppended) {
                return false;
            }

            //
            // Record where the file should go.  Then open a
            // SequenceFile.Writer to record the set of items
            // we append to each table.
            //
            outputPaths.add(outputPath);
            tables.add(new SequenceFile.Writer(nfs, outputPath + ".unsorted", MD5Hash.class, FetchListEntry.class));
            return true;
        }

        /**
         * Add FetchListEntry items to one of the tables.
         * Choose the table based on a hash of the URL domain name.
         */
        public synchronized boolean append(FetchListEntry newEntry) throws IOException {
            hasAppended = true;
            if (appendCounts == null) {
                appendCounts = new long[outputPaths.size()];
            }

            Page fetchPage = newEntry.getPage();

            // Extract the hostname from the URL
            String host = null;
            try {
                host = new URL(fetchPage.getURL().toString()).getHost().toLowerCase();
            } catch (MalformedURLException e) {
                // ignore bad URLs
                return false;
            }

            // Figure out which table is getting the item
            MD5Hash hash = MD5Hash.digest(host);
            int index = Math.abs(hash.hashCode()^seed) % tables.size();

            // Write it down and return
            SequenceFile.Writer writer = (SequenceFile.Writer) tables.elementAt(index);
            writer.append(fetchPage.getMD5(), newEntry);
            appendCounts[index]++;

            return true;
        }

        /**
         * Close down the TableSet, so there are no more FetchListEntries
         * expected.  We now:
         * a) Close down all the SequenceFile.Writer objects.
         * b) Sort each file
         * c) Read each newly-sorted file and copy to an ArrayFile
         */
        public synchronized void close() throws IOException {
            hasAppended = true;

            // A) Close all the SequenceFile.Writers
            for (Enumeration e = tables.elements(); e.hasMoreElements(); ) {
                ((SequenceFile.Writer) e.nextElement()).close();
            }

            // B) Sort the edit-files
            SequenceFile.Sorter sorter = new SequenceFile.Sorter(nfs, new MD5Hash.Comparator(), FetchListEntry.class);

            //
            // Iterate through each unsorted file.  Sort it (while
            // measuring the time taken) and upon completion delete
            // the unsorted version.
            //
            long totalEntries = 0;
            double totalTime = 0;
            int i = 0;
            for (Enumeration e = outputPaths.elements(); e.hasMoreElements(); i++) {
                String name = (String) e.nextElement();
                String unsortedName = name + ".unsorted";

                long localStart = System.currentTimeMillis();
                sorter.sort(unsortedName, name + ".sorted");
                long localEnd = System.currentTimeMillis();

                if (appendCounts != null) {
                    double localSecs = ((localEnd - localStart) / 1000.0);
                    LOG.info("Processing " + unsortedName + ": Sorted " + appendCounts[i] + " entries in " + localSecs + " seconds.");
                    LOG.info("Processing " + unsortedName + ": Sorted " + (appendCounts[i] / localSecs) + " entries/second");

                    totalEntries += appendCounts[i];
                    totalTime += localSecs;
                }

                nfs.delete(new File(name + ".unsorted"));
            }

            LOG.info("Overall processing: Sorted " + totalEntries + " entries in " + totalTime + " seconds.");
            LOG.info("Overall processing: Sorted " + (totalTime / totalEntries) + " entries/second");

            // C) Read in each newly-sorted file.  Copy to an ArrayFile.
            for (Enumeration e = outputPaths.elements(); e.hasMoreElements(); ) {
                String name = (String) e.nextElement();
                SequenceFile.Reader reader = new SequenceFile.Reader(nfs, name + ".sorted");
                ArrayFile.Writer af = new ArrayFile.Writer(nfs, name, FetchListEntry.class);
                try {
                    MD5Hash key = new MD5Hash();
                    FetchListEntry fle = new FetchListEntry();
                    while (reader.next(key, fle)) {
                        af.append(fle);
                    }
                } finally {
                    af.close();
                    reader.close();
                    nfs.delete(new File(name + ".sorted"));
                }
            }
        }
    }

    /*************************************
     * SortableScore is just a WritableComparable Float!
     *************************************/
    public static class SortableScore implements WritableComparable {
        float score;

        /**
         */
        public SortableScore() {
        }

        /**
         */
        public void set(float score) {
            this.score = score;
        }

        /**
         */
        public float getFloat() {
            return score;
        }


        ////////
        // WritableComparable
        ////////

        /**
         * Sort them in descending order!
         */
        public int compareTo(Object o) {
            SortableScore otherScore = (SortableScore) o;

            if (score < otherScore.score) {
                return 1;
            } else if (score == otherScore.score) {
                return 0;
            } else {
                return -1;
            }
        }

        /**
         */
        public void write(DataOutput out) throws IOException {
            out.writeFloat(score);
        }

        /**
         */
        public void readFields(DataInput in) throws IOException {
            this.score = in.readFloat();
        }
    }

    /**
     * FetchListTool takes a page db, and emits a RECNO-based
     * subset of it.
     */
    public FetchListTool(NutchFileSystem nfs, File dbDir, boolean refetchOnly, float cutoffScore, int seed) throws IOException, FileNotFoundException {
        this.nfs = nfs;
        this.dbDir = dbDir;
        this.refetchOnly = refetchOnly;
        this.cutoffScore = cutoffScore;
        this.seed = seed;
    }

    /**
     * Spit out several fetchlists, so that we can fetch across
     * several machines.  
     */
    public void emitMultipleLists(File dir, int numLists, long topN, long curTime) throws IOException {
        //
        // Create tables (and directories) for each fetchlist we want.
        // Add them all to a TableSet object.
        //
        TableSet tables = new TableSet();
        try {
            String datePrefix = getDate();

            File workingDir = new File(dir, "tmp_" + getDate());
            nfs.mkdirs(workingDir);
            try {
                for (int i = 0; i < numLists; i++) {
                    File subdir = new File(dir, datePrefix + "-" + i);
                    nfs.mkdirs(subdir);
                    File file = new File(subdir, FetchListEntry.DIR_NAME);
                    tables.add(file.getPath());
                }

                // Now go through the fetchlist.
                emitFetchList(tables, workingDir, topN, curTime);
            } finally {
                FileUtil.fullyDelete(nfs, workingDir);
            }
        } finally {
            tables.close();
        }
    }

    /**
     * Spit out the fetchlist, to a BDB at the indicated filename.
     */
    public void emitFetchList(File segmentDir, long topN, long curTime) throws IOException {
        TableSet tables = new TableSet();
        File workingDir = new File(segmentDir, "tmp_" + getDate());
        nfs.mkdirs(workingDir);
        File subdir = new File(segmentDir, getDate());
        nfs.mkdirs(subdir);

        try {
            tables.add(new File(subdir, FetchListEntry.DIR_NAME).getPath());
        
            try {
                emitFetchList(tables, workingDir, topN, curTime);
            } finally {
                tables.close();
            }
        } finally {
            FileUtil.fullyDelete(nfs, workingDir);
        }
    }

    private static String getDate() {
        return new SimpleDateFormat("yyyyMMddHHmmss").format
            (new Date(System.currentTimeMillis()));
    }

    /**
     * Emit the fetchlist, with the given TableSet.  The TableSet is
     * responsible for actually appending the item to the output file, 
     * which is from this function.
     */
    void emitFetchList(TableSet tables, File workingDir, long topN, long curTime) throws IOException {
        // Iterate through all the Pages, by URL.  Iterating
        // through by URL means we can save disk seeks when
        // calling webdb.getLinks(URL).  
        //
        // However, we don't really want the output to be in URL-ordered
        // format.  We would like the output to be URL-randomized, which
        // an MD5-ordering preserves nicely.  But we assume here that
        // TableSet will do that randomizing for us.  We just need to
        // make sure we give it a good sampling of our data.  (That is,
        // if we are giving TableSet fewer than the max-possible items,
        // we should make sure the items come evenly from all over the
        // db.)
        //
        long count = 0;
        TreeMap anchorTable = new TreeMap();
        Vector unknownDomainLinks = new Vector();

        //
        // Create a comparator that matches the domainIDs for
        // Link objects.
        //
        Comparator domainComparator = new Comparator() {
            public int compare(Object o1, Object o2) {
                Link l1 = (Link) o1;
                Link l2 = (Link) o2;
                if (l1.getDomainID() < l2.getDomainID()) {
                    return -1;
                } else if (l1.getDomainID() == l2.getDomainID()) {
                    return 0;
                } else {
                    return 1;
                }
            }
        };

        //
        // Go through all the pages by URL.  Filter the ones
        // we really don't want, and save the others for possible
        // emission.
        //
        SortableScore curScore = new SortableScore();
        File unsortedFile = new File(workingDir, TOP_N_SORTER + ".unsorted");
        SequenceFile.Writer writer = new SequenceFile.Writer(nfs, unsortedFile.getPath(), SortableScore.class, FetchListEntry.class);
        try {
            IWebDBReader webdb = new WebDBReader(nfs, dbDir);
            WebDBAnchors dbAnchors = new WebDBAnchors(webdb);
            try {
                for (Enumeration e = webdb.pages(); e.hasMoreElements(); count++) {
                    // Grab the next Page.
                    Page page = (Page) e.nextElement();
                    boolean shouldFetch = true;

                    if (((count % 10000) == 0) && (count != 0)) {
                        LOG.info("Processing page " + count + "...");
                    }

                    //
                    // Don't emit it if the Page's score doesn't meet
                    // our cutoff value
                    //
                    if ((cutoffScore >= 0) && (page.getScore() < cutoffScore)) {
                        continue;
                    }

                    //
                    // If the item is not yet ready to be fetched, move on.
                    //
                    // Also, if getNextFetchTime is set to Long.MAX_VALUE,
                    // then it should never be fetched.
                    //
                    if (page.getNextFetchTime() > curTime ||
                        page.getNextFetchTime() == Long.MAX_VALUE) {
                        continue;
                    }

                    //
                    // If we're in refetchOnly mode, set shouldFetch to FALSE
                    // for any Pages whose URL's MD5 is the same as the 
                    // listed MD5.  That indicates that no content has been 
                    // downloaded in the past.
                    //
                    if (refetchOnly) {
                        MD5Hash urlHash = MD5Hash.digest(page.getURL());
                        if (page.getMD5().equals(urlHash)) {
                            shouldFetch = false;
                        }
                    }

                    // get the links from the db
                    String[] anchors = dbAnchors.getAnchors(page.getURL());

                    // 
                    // Last, add the FetchListEntry to a file so we can
                    // sort by score.  Be sure to modify the Page's
                    // fetchtime; this allows us to soon generate
                    // another fetchlist which would not include this
                    // Page.  That's helpful because with two distinct
                    // fetchlists, it should be possible to fetch and
                    // perform dbupdate at the same time.
                    //
                    // Optionally set the score by the log of number of 
                    // incoming anchors.
                    curScore.set(scoreByLinkCount ?
                            (float)Math.log(anchors.length+1) : page.getScore());
                    page.setNextFetchTime(System.currentTimeMillis() + FETCH_GENERATION_DELAY_MS);
                    writer.append(curScore, new FetchListEntry(shouldFetch, page, anchors));
                }
            } finally {
                webdb.close();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            writer.close();
        }

        //
        // The next step is to sort the file we created above.
        // after being sorted, we add the "topN" items to the
        // TableSet.
        //
        File sortedFile = new File(workingDir, TOP_N_SORTER + ".sorted");
        SequenceFile.Sorter topNSorter = new SequenceFile.Sorter(nfs, SortableScore.class, FetchListEntry.class);
        topNSorter.sort(unsortedFile.getPath(), sortedFile.getPath());

        //
        // Last of all, add the topN items to the table set.
        //
        // This is also where we rewrite the WebDB - we need to do
        // this so we can modify the "date" field.  Rewriting the
        // db can be expensive, but it's that modification that will
        // allow us to interleave fetching and db-update.
        //
        WebDBWriter dbwriter = new WebDBWriter(nfs, dbDir);
        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(nfs, sortedFile.getPath());
            try {
                SortableScore key = new SortableScore();
                FetchListEntry value = new FetchListEntry();
                while (topN > 0 && reader.next(key, value)) {
                    tables.append(value);
                    topN--;

                    //
                    // Modify the Page in the webdb so that its date
                    // is set forward a week.  This way, we can have
                    // generate two consecutive different fetchlists
                    // without an intervening update.  So, we generate
                    // lists A and B, and start fetching A.  Upon
                    // completion, we use A to update the db, and start
                    // fetching B.  This way we have simultaneous
                    // dbupdate and page fetch, which should double
                    // our throughput.
                    //
                    dbwriter.addPage(value.getPage());
                }
            } finally {
                reader.close();
            }
        } finally {
            dbwriter.close();
        }
    }

    /**
     * Generate a fetchlist from the pagedb and linkdb
     */
    public static void main(String argv[]) throws IOException, FileNotFoundException {
        if (argv.length < 2) {
            System.out.println("Usage: FetchListTool (-local | -ndfs <namenode:port>) <db>  <segment_dir> [-refetchonly] [-topN N] [-cutoff cutoffscore] [-numFetchers numFetchers] [-adddays numDays]");
            return;
        }

        //
        // Required args
        //
        int i = 0;
        NutchFileSystem nfs = NutchFileSystem.parseArgs(argv, i);
        File dbDir = new File(argv[i++]);
        File segmentDir = new File(argv[i++]);
        long curTime = System.currentTimeMillis();

        //
        // Optional args
        //
        boolean refetchOnly = false;
        long topN = Long.MAX_VALUE;
        float cutoffScore = -1.0f;
        int numFetchers = 1;
        int seed = new Random().nextInt();


        try {
            for (; i < argv.length; i++) {
                if ("-refetchonly".equals(argv[i])) {
                    refetchOnly = true;
                } else if ("-topN".equals(argv[i])) {
                    if (i+1 < argv.length) {
                        topN = Long.parseLong(argv[i+1]);
                        i++;
                    } else {
                        System.out.println("No argument present for -topN");
                        return;
                    }
                } else if ("-cutoff".equals(argv[i])) {
                    if (i+1 < argv.length) {
                        cutoffScore = Float.parseFloat(argv[i+1]);
                        i++;
                    } else {
                        System.out.println("No argument present for -cutoffscore");
                        return;
                    }
                } else if ("-numFetchers".equals(argv[i])) {
                    if (i+1 < argv.length) {
                        numFetchers = Integer.parseInt(argv[i+1]);
                        i++;
                    } else {
                        System.out.println("No argument present for -numFetchers");
                        return;
                    }
                } else if ("-adddays".equals(argv[i])) {
                    if (i+1 < argv.length) {
                        long numDays = Integer.parseInt(argv[i+1]);
                        curTime += numDays * 1000L * 60 * 60 * 24;
                    } else {
                        System.out.println("No argument present for -adddays");
                        return;
                    }
                }
            }
        } catch (NumberFormatException nfe) {
            System.out.println("Badly-formatted number:: " + nfe);
            return;
        }


        //
        // Finally, start things up.
        //
        LOG.info("FetchListTool started");
        if (topN != Long.MAX_VALUE) {
            LOG.info("topN:" + topN);
        }
        if (cutoffScore >= 0) {
            LOG.info("cutoffscore:" + cutoffScore);
        }
        if (numFetchers > 1) {
            LOG.info("seed:" + seed);
        }

        FetchListTool flt = new FetchListTool(nfs, dbDir, refetchOnly, cutoffScore, seed);
        if (numFetchers > 1) {
            flt.emitMultipleLists(segmentDir, numFetchers, topN, curTime);
        } else {
            flt.emitFetchList(segmentDir, topN, curTime);
        }
        nfs.close();
        LOG.info("FetchListTool completed");
    }
}
