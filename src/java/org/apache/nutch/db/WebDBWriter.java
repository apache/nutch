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
import org.apache.nutch.pagedb.*;
import org.apache.nutch.linkdb.*;

/***************************************************
 * This is a wrapper class that allows us to reorder
 * write operations to the linkdb and pagedb.  It is
 * useful only for objects like UpdateDatabaseTool,
 * which just does writes.
 *
 * The WebDBWriter is a traditional single-pass database writer.
 * It does not cache any instructions to disk (but it does
 * in memory, with possible resorting).  It certainly does
 * nothing in a distributed fashion.
 *
 * There are other implementors of IWebDBWriter that do
 * all that fancy stuff.
 *
 * @author Mike Cafarella
 *************************************************/
public class WebDBWriter implements IWebDBWriter {
    static final Logger LOG = LogFormatter.getLogger("org.apache.nutch.db.WebDBWriter");
    static final byte CUR_VERSION = 0;

    // db opcodes
    static final byte ADD_PAGE = 0;
    static final byte ADD_PAGE_WITH_SCORE = 1;
    static final byte ADD_PAGE_IFN_PRESENT = 2;
    static final byte DEL_PAGE = 3;
    static final int ADD_LINK = 0;
    static final int DEL_LINK = 1;
    static final int DEL_SINGLE_LINK = 2;

    // filenames
    static final String PAGES_BY_URL = "pagesByURL";
    static final String PAGES_BY_MD5 = "pagesByMD5";
    static final String LINKS_BY_URL = "linksByURL";
    static final String LINKS_BY_MD5 = "linksByMD5";
    static final String STATS_FILE = "stats";

    // Result codes for page-url comparisons
    static final int NO_OUTLINKS = 0;
    static final int HAS_OUTLINKS = 1;
    static final int LINK_INVALID = 2;

    /********************************************
     * PageInstruction holds an operation over a Page.
     *********************************************/
    public static class PageInstruction implements WritableComparable {
        byte opcode;
        boolean hasLink;
        Page page;
        Link link;

        /**
         */
        public PageInstruction() {}

        /**
         */
        public PageInstruction(Page page, int opcode) {
            set(page, opcode);
        }

        /**
         */
        public PageInstruction(Page page, Link link, int opcode) {
            set(page, link, opcode);
        }

        /**
         * Init from another PageInstruction object.
         */
        public void set(PageInstruction that) {
            this.opcode = that.opcode;

            if (this.page == null) {
                this.page = new Page();
            }
            this.page.set(that.page);

            if (this.link == null) {
                this.link = new Link();
            }
            this.hasLink = that.hasLink;
            if (this.hasLink) {
                this.link.set(that.link);
            }
        }

        /**
         * Init PageInstruction with no Link
         */
        public void set(Page page, int opcode) {
            this.opcode = (byte) opcode;
            this.page = page;
            this.hasLink = false;
            this.link = null;
        }

        /**
         * Init PageInstruction with a Link
         */         
        public void set(Page page, Link link, int opcode) {
            this.opcode = (byte) opcode;
            this.page = page;
            this.hasLink = true;
            this.link = link;
        }

        //
        // WritableComparable
        //
        public int compareTo(Object o) {
            int pageResult = this.page.compareTo(((PageInstruction) o).page);
            if (pageResult != 0) {
                return pageResult;
            } else {
                return this.opcode - (((PageInstruction) o).opcode);
            }
        }
        public void write(DataOutput out) throws IOException {
            out.writeByte(opcode);
            page.write(out);
            out.writeByte(hasLink ? 1 : 0);
            if (hasLink) {
                link.write(out);
            }
        }
        public void readFields(DataInput in) throws IOException {
            opcode = in.readByte();
            if (page == null) {
                page = new Page();
            }
            page.readFields(in);
            
            if (link == null) {
                link = new Link();
            }
            hasLink = (1 == in.readByte());
            if (hasLink) {
                link.readFields(in);
            }
        }
        public Page getPage() {
            return page;
        }
        public Link getLink() {
            if (hasLink) {
                return link;
            } else {
                return null;
            }
        }
        public int getInstruction() {
            return opcode;
        }

        /**
         * Sorts the instruction first by Page, then by opcode.
         */
        public static class PageComparator extends WritableComparator {
            private static final Page.Comparator PAGE_COMPARATOR =
            new Page.Comparator();

            public PageComparator() { super(PageInstruction.class); }

            /** Optimized comparator. */
            public int compare(byte[] b1, int s1, int l1,
                               byte[] b2, int s2, int l2) {
                int opcode1 = b1[s1];
                int opcode2 = b2[s2];
                int c = PAGE_COMPARATOR.compare(b1, s1+1, l1-1, b2, s2+1, l2-1);
                if (c != 0)
                    return c;
                return opcode1 - opcode2;
            }
        }
 
        /*****************************************************
         * Sorts the instruction first by url, then by opcode.
         *****************************************************/
        public static class UrlComparator extends WritableComparator {
            private static final Page.UrlComparator PAGE_COMPARATOR =
            new Page.UrlComparator();

            public UrlComparator() { super(PageInstruction.class); }

            /**
             * We need to sort by ordered URLs.  First, we sort by
             * URL, then by opcode.
             */
            public int compare(WritableComparable a, WritableComparable b) {
                PageInstruction instructionA = (PageInstruction)a;
                PageInstruction instructionB = (PageInstruction)b;
                Page pageA = instructionA.getPage();
                Page pageB = instructionB.getPage();

                int result = pageA.getURL().compareTo(pageB.getURL());
                if (result != 0) {
                    return result;
                } else {
                    return instructionA.opcode - instructionB.opcode;
                }
            }

            /** 
             * Optimized comparator. 
             */
            public int compare(byte[] b1, int s1, int l1,
                               byte[] b2, int s2, int l2) {
                int opcode1 = b1[s1];
                int opcode2 = b2[s2];
                int c = PAGE_COMPARATOR.compare(b1, s1+1, l1-1, b2, s2+1, l2-1);
                if (c != 0)
                    return c;
                return opcode1 - opcode2;
            }
        }
    }

    /********************************************************
     * PageInstructionWriter very efficiently writes a 
     * PageInstruction to a SequenceFile.Writer.  Much better
     * than calling "writer.append(new PageInstruction())"
     ********************************************************/
    public static class PageInstructionWriter {
        PageInstruction pi = new PageInstruction();

        /**
         */
        public PageInstructionWriter() {
        }

        /**
         * Append the PageInstruction info to the indicated SequenceFile,
         * and keep the PI for later reuse.
         */
        public synchronized void appendInstructionInfo(SequenceFile.Writer writer, Page page, int opcode, Writable val) throws IOException {
            pi.set(page, opcode);
            writer.append(pi, val);
        }

        /**
         * Append the PageInstruction info to the indicated SequenceFile,
         * and keep the PI for later reuse.
         */
        public synchronized void appendInstructionInfo(SequenceFile.Writer writer, Page page, Link link, int opcode, Writable val) throws IOException {
            pi.set(page, link, opcode);
            writer.append(pi, val);
        }
    }

    /*************************************************************
     * Reduce multiple instructions for a given url to the single effective
     * instruction.  ADD is prioritized highest, then ADD_IFN_PRESENT, and then
     * DEL.  Not coincidentally, this is opposite the order they're sorted in.
     **************************************************************/
    private static class DeduplicatingPageSequenceReader {
        SequenceFile.Reader edits;
        PageInstruction current = new PageInstruction();
        UTF8 currentUrl = new UTF8();
        boolean haveCurrent;

        /**
         */
        public DeduplicatingPageSequenceReader(SequenceFile.Reader edits) throws IOException {
            this.edits = edits;
            this.haveCurrent = edits.next(current, NullWritable.get());
        }

        /**
         */
        public boolean next(PageInstruction result) throws IOException {
            if (!haveCurrent) {
                return false;
            }
        
            currentUrl.set(current.getPage().getURL());
            result.set(current); // take the first instruction

            do {
                // skip the rest
            } while ((haveCurrent = edits.next(current, NullWritable.get())) &&
                     currentUrl.compareTo(current.getPage().getURL()) == 0);
            return true;
        }
    }


    /*************************************************
     * Holds an instruction over a Link.
     *************************************************/
    public static class LinkInstruction implements WritableComparable {
        Link link;
        int instruction;

        /**
         */
        public LinkInstruction() {
        }

        /**
         */
        public LinkInstruction(Link link, int instruction) {
            set(link, instruction);
        }

        /**
         * Re-init from another LinkInstruction's info.
         */
        public void set(LinkInstruction that) {
            this.instruction = that.instruction;
          
            if (this.link == null)
                this.link = new Link();

            this.link.set(that.link);
        }

        /**
         * Re-init with a Link and an instruction
         */
        public void set(Link link, int instruction) {
            this.link = link;
            this.instruction = instruction;
        }

        //
        // WritableComparable
        //
        public int compareTo(Object o) {
            return this.link.compareTo(((LinkInstruction) o).link);
        }
        public void write(DataOutput out) throws IOException {
            out.writeByte(instruction);
            link.write(out);
        }
        public void readFields(DataInput in) throws IOException {
            this.instruction = in.readByte();
            if (link == null)
                link = new Link();
            link.readFields(in);
        }
        public Link getLink() {
            return link;
        }
        public int getInstruction() {
            return instruction;
        }

        /*******************************************************
         * Sorts the instruction first by Md5, then by opcode.
         *******************************************************/
        public static class MD5Comparator extends WritableComparator {
            private static final Link.MD5Comparator MD5_COMPARATOR =
            new Link.MD5Comparator();

            public MD5Comparator() { super(LinkInstruction.class); }

            public int compare(WritableComparable a, WritableComparable b) {
                LinkInstruction instructionA = (LinkInstruction)a;
                LinkInstruction instructionB = (LinkInstruction)b;
                return instructionA.link.md5Compare(instructionB.link);
            }

            /** Optimized comparator. */
            public int compare(byte[] b1, int s1, int l1,
                               byte[] b2, int s2, int l2) {
                return MD5_COMPARATOR.compare(b1, s1+1, l1-1, b2, s2+1, l2-1);
            }
        }
 
        /*********************************************************
         * Sorts the instruction first by url, then by opcode.
         *********************************************************/
        public static class UrlComparator extends WritableComparator {
            private static final Link.UrlComparator URL_COMPARATOR =
            new Link.UrlComparator();

            public UrlComparator() { super(LinkInstruction.class); }

            public int compare(WritableComparable a, WritableComparable b) {
                LinkInstruction instructionA = (LinkInstruction)a;
                LinkInstruction instructionB = (LinkInstruction)b;
                return instructionA.link.urlCompare(instructionB.link);

            }

            /** 
             * Optimized comparator. 
             */
            public int compare(byte[] b1, int s1, int l1,
                               byte[] b2, int s2, int l2) {
                return URL_COMPARATOR.compare(b1, s1+1, l1-1, b2, s2+1, l2-1);
            }
        }
    }

    /*******************************************************
     * LinkInstructionWriter very efficiently writes a
     * LinkInstruction to a SequenceFile.Writer.  Much better
     * than calling "writer.append(new LinkInstruction())"
     ********************************************************/
    public static class LinkInstructionWriter {
        LinkInstruction li = new LinkInstruction();

        /**
         */
        public LinkInstructionWriter() {
        }

        /**
         * Append the LinkInstruction info to the indicated SequenceFile
         * and keep the LI for later reuse.
         */
        public synchronized void appendInstructionInfo(SequenceFile.Writer writer, Link link, int opcode, Writable val) throws IOException {
            li.set(link, opcode);
            writer.append(li, val);
        }
    }

    /********************************************************
     * This class deduplicates link operations.  We want to 
     * sort by MD5, then by URL.  But all operations
     * should be unique.
     *********************************************************/
    class DeduplicatingLinkSequenceReader {
        Link currentKey = new Link();
        LinkInstruction current = new LinkInstruction();
        SequenceFile.Reader edits;
        boolean haveCurrent;

        /**
         */
        public DeduplicatingLinkSequenceReader(SequenceFile.Reader edits) throws IOException {
            this.edits = edits;
            this.haveCurrent = edits.next(current, NullWritable.get());
        }


        /**
         * The incoming stream of edits is sorted first by MD5, then by URL.
         * MD5-only values always come before MD5+URL.
         */
        public boolean next(LinkInstruction key) throws IOException {
            if (! haveCurrent) {
                return false;
            }

            currentKey.set(current.getLink());
            
            do {
                key.set(current);
            } while ((haveCurrent = edits.next(current, NullWritable.get())) &&
                     currentKey.compareTo(current.getLink()) == 0);
            return true;
        }
    }


    /**************************************************
     * The CloseProcessor class is used when we close down
     * the webdb.  We give it the path, members, and class values
     * needed to apply changes to any of our 4 data tables.
     * 
     * This is an abstract class.  Each subclass must define
     * the exact merge procedure.  However, file-handling
     * and edit-processing is standardized as much as possible.
     *
     **************************************************/
    private abstract class CloseProcessor {
        String basename;
        MapFile.Reader oldDb;
        SequenceFile.Writer editWriter;
        SequenceFile.Sorter sorter;
        WritableComparator comparator;
        Class keyClass, valueClass;
        long itemsWritten = 0;

        /**
         * Store away these members for later use.
         */
        CloseProcessor(String basename, MapFile.Reader oldDb, SequenceFile.Writer editWriter, SequenceFile.Sorter sorter, WritableComparator comparator, Class keyClass, Class valueClass) {
            this.basename = basename;
            this.oldDb = oldDb;
            this.editWriter = editWriter;
            this.sorter = sorter;
            this.comparator = comparator;
            this.keyClass = keyClass;
            this.valueClass = valueClass;
        }

        /**
         * Perform the shutdown sequence for this Processor.
         * There is a lot of file-moving and edit-sorting that
         * is common across all the 4 tables.
         *
         * Returns how many items were written out by this close().
         */
        long closeDown(File workingDir, File outputDir, long numEdits) throws IOException {
            File editsFile = new File(workingDir, basename + ".out");
            File newDbFile = new File(outputDir, basename);
            File sortedEditsFile = new File(editsFile.getPath() + ".sorted");
            editWriter.close();

            // If there are edits, then process them.
            if (numEdits != 0) {
                // Sort the edits
                long startSort = System.currentTimeMillis();
                sorter.sort(editsFile.getPath(), sortedEditsFile.getPath());
                // sorter.close();
                long endSort = System.currentTimeMillis();
                LOG.info("Processing " + basename + ": Sorted " + numEdits + " instructions in " + ((endSort - startSort) / 1000.0) + " seconds.");
                LOG.info("Processing " + basename + ": Sorted " + (numEdits / ((endSort - startSort) / 1000.0)) + " instructions/second");
            
                // Rename appropriately
                fs.delete(editsFile);
                fs.rename(sortedEditsFile, editsFile);

                // Read the sorted edits
                SequenceFile.Reader sortedEdits = new SequenceFile.Reader(fs, editsFile.getPath());

                // Create a brand-new output db for the integrated data
                MapFile.Writer newDb = (comparator == null) ? new MapFile.Writer(fs, newDbFile.getPath(), keyClass, valueClass) : new MapFile.Writer(fs, newDbFile.getPath(), comparator, valueClass);

                // Iterate through the edits, and merge changes with existing
                // db into the brand-new file
                oldDb.reset();
            
                // Merge the edits.  We did it!
                long startMerge = System.currentTimeMillis();
                mergeEdits(oldDb, sortedEdits, newDb);
                long endMerge = System.currentTimeMillis();
                LOG.info("Processing " + basename + ": Merged to new DB containing " + itemsWritten + " records in " + ((endMerge - startMerge) / 1000.0) + " seconds");
                LOG.info("Processing " + basename + ": Merged " + (itemsWritten / ((endMerge - startMerge) / 1000.0)) + " records/second");

                // Close down readers, writers
                sortedEdits.close();
                newDb.close();
            } else {
                // Otherwise, simply copy the file into place,
                // without all the processing overhead.
                long startCopy = System.currentTimeMillis();
                File curFile = new File(dbFile, basename);
                FileUtil.recursiveCopy(fs, curFile, newDbFile);
                long endCopy = System.currentTimeMillis();

                LOG.info("Processing " + basename + ": Copied file (" + newDbFile.length()+ " bytes) in " + ((endCopy - startCopy) / 1000.0) + " secs.");
            }

            // Delete the now-consumed edits file to save space
            fs.delete(editsFile);

            return itemsWritten;
        }

        /**
         * The loop that actually applies the changes and writes to
         * a new db.  This is different for every subclass!
         */
        abstract void mergeEdits(MapFile.Reader db, SequenceFile.Reader edits, MapFile.Writer newDb) throws IOException;
    }

    /***
     * The PagesByURLProcessor is used during close() time for
     * the pagesByURL table.  We instantiate one of these, and it
     * takes care of the entire shutdown process.
     */
    private class PagesByURLProcessor extends CloseProcessor {
        SequenceFile.Writer futureEdits;

        /**
         * We store "futureEdits" so we can write edits for the
         * next table-db step
         */
        PagesByURLProcessor(MapFile.Reader db, SequenceFile.Writer editWriter, SequenceFile.Writer futureEdits) {
            super(PAGES_BY_URL, db, editWriter, new SequenceFile.Sorter(fs, new PageInstruction.UrlComparator(), NullWritable.class), new UTF8.Comparator(), null, Page.class);
            this.futureEdits = futureEdits;
        }

        /**
         * Merge the existing db with the edit-stream into a brand-new file.
         */
        void mergeEdits(MapFile.Reader db, SequenceFile.Reader sortedEdits, MapFile.Writer newDb) throws IOException {
            // Create the keys and vals we'll be using
            DeduplicatingPageSequenceReader edits = new DeduplicatingPageSequenceReader(sortedEdits);
            WritableComparable readerKey = new UTF8();
            Page readerVal = new Page();
            PageInstruction editItem = new PageInstruction();
            int futureOrdering = 0;

            // Read the first items from both streams
            boolean hasEntries = db.next(readerKey, readerVal);
            boolean hasEdits = edits.next(editItem);

            // As long as we have both edits and entries, we need to
            // interleave them....
            while (hasEntries && hasEdits) {
                int comparison = readerKey.compareTo(editItem.getPage().getURL());
                int curInstruction = editItem.getInstruction();

                // Perform operations
                if ((curInstruction == ADD_PAGE) ||
                    (curInstruction == ADD_PAGE_WITH_SCORE) ||
                    (curInstruction == ADD_PAGE_IFN_PRESENT)) {

                    if (comparison < 0) {
                        // Write readerKey, just passing it along.
                        // Don't process the edit yet.
                        newDb.append(readerKey, readerVal);
                        itemsWritten++;
                        hasEntries = db.next(readerKey, readerVal);
                    } else if (comparison == 0) {
                        // The keys are equal.  If the instruction 
                        // is ADD_PAGE, we write the edit's key and 
                        // replace the old one.
                        //
                        // Otherwise, if it's ADD_IFN_PRESENT, 
                        // keep the reader's item intact.
                        //
                        if ((curInstruction == ADD_PAGE) ||
                            (curInstruction == ADD_PAGE_WITH_SCORE)) {
                            // An ADD_PAGE with an identical pair
                            // of pages replaces the existing one.
                            // We may need to note the fact for
                            // Garbage Collection.
                            //
                            // This happens in three stages.  
                            // 1.  We write necessary items to the future
                            //     edits-list.
                            //
                            pagesByMD5Edits++;

                            // If this is a replacing add, we don't want
                            // to disturb the score from the old Page!  This,
                            // way, we can run some link analysis scoring
                            // while the new Pages are being fetched and
                            // not lose the info when a Page is replaced.
                            //
                            // If it is an ADD_PAGE_WITH_SCORE, then we 
                            // go ahead and replace the old one.
                            //
                            // Either way, from now on we treat it
                            // as an ADD_PAGE
                            //
                            Page editItemPage = editItem.getPage();

                            if (curInstruction == ADD_PAGE) {
                                editItemPage.setScore(readerVal.getScore(), readerVal.getNextScore());
                            }

                            piwriter.appendInstructionInfo(futureEdits, editItemPage, ADD_PAGE, NullWritable.get());

                            //
                            // 2.  We write the edit-page to *this* table.
                            //
                            newDb.append(editItemPage.getURL(), editItemPage);

                            //
                            // 3.  We want the ADD in the next step (the
                            //     MD5-driven table) to be a "replacing add".
                            //     But that won't happen if the readerItem and
                            //     the editItem Pages are not identical.
                            //     (In this scenario, that means their URLs
                            //     are the same, but their MD5s are different.)
                            //     So, we need to explicitly handle that
                            //     case by issuing a DELETE for the now-obsolete
                            //     item.
                            if (editItemPage.compareTo(readerVal) != 0) {
                                pagesByMD5Edits++;
                                piwriter.appendInstructionInfo(futureEdits, readerVal, DEL_PAGE, NullWritable.get());
                            }

                            itemsWritten++;

                            // "Delete" the readerVal by skipping it.
                            hasEntries = db.next(readerKey, readerVal);
                        } else {
                            // ADD_PAGE_IFN_PRESENT.  We only add IF_NOT
                            // present.  And it was present!  So, we treat 
                            // this case like we treat a no-op.
                            // Just move to the next edit.
                        }
                        // In either case, we process the edit.
                        hasEdits = edits.next(editItem);

                    } else if (comparison > 0) {
                        // We have inserted a Page that's before some
                        // entry in the existing database.  So, we just
                        // need to write down the Page from the Edit file.
                        // It's like the above case, except we don't tell
                        // the future-edits to delete anything.
                        //
                        // 1.  Write the item down for the future.
                        pagesByMD5Edits++;

                        //
                        // If this is an ADD_PAGE_IFN_PRESENT, then
                        // we may also have a Link we have to take care of!
                        //
                        if (curInstruction == ADD_PAGE_IFN_PRESENT) {
                            Link editLink = editItem.getLink();
                            if (editLink != null) {
                                addLink(editLink);
                            }
                        }
                        piwriter.appendInstructionInfo(futureEdits, editItem.getPage(), ADD_PAGE, NullWritable.get());

                        //
                        // 2.  Write the edit-page to *this* table
                        newDb.append(editItem.getPage().getURL(), editItem.getPage());
                        itemsWritten++;

                        // Process the edit
                        hasEdits = edits.next(editItem);
                    }
                } else if (curInstruction == DEL_PAGE) {
                    if (comparison < 0) {
                        // Write the readerKey, just passing it along.
                        // We don't process the edit yet.
                        newDb.append(readerKey, readerVal);
                        itemsWritten++;
                        hasEntries = db.next(readerKey, readerVal);
                    } else if (comparison == 0) {
                        // Delete it!  We can only delete one item
                        // at a time, as all URLs are unique.
                        // 1.  Tell the future-edits what page will need to
                        //     be deleted.
                        pagesByMD5Edits++;
                        piwriter.appendInstructionInfo(futureEdits, readerVal, DEL_PAGE, NullWritable.get());

                        //
                        // 2.  "Delete" the entry by skipping the Reader
                        //     key.
                        hasEntries = db.next(readerKey, readerVal);

                        // Process the edit
                        hasEdits = edits.next(editItem);
                    } else if (comparison > 0) {
                        // Ignore it.  We tried to delete an item that's
                        // not here.
                        hasEdits = edits.next(editItem);
                    }
                }
            }

            // Now we have only edits.  No more preexisting items!
            while (! hasEntries && hasEdits) {
                int curInstruction = editItem.getInstruction();
                if (curInstruction == ADD_PAGE ||
                    curInstruction == ADD_PAGE_WITH_SCORE ||
                    curInstruction == ADD_PAGE_IFN_PRESENT) {
                    // No more reader entries, so ADD_PAGE_IFN_PRESENT
                    // is treated like a simple ADD_PAGE.

                    // 1.  Tell the future edits-list about this new item
                    pagesByMD5Edits++;
                    
                    //
                    // If this is an ADD_PAGE_IFN_PRESENT, then
                    // we may also have a Link we have to take care of!
                    //
                    if (curInstruction == ADD_PAGE_IFN_PRESENT) {
                        Link editLink = editItem.getLink();
                        if (editLink != null) {
                            addLink(editLink);
                        }
                    }
                    piwriter.appendInstructionInfo(futureEdits, editItem.getPage(), ADD_PAGE, NullWritable.get());

                    // 2.  Write the edit page to this table.
                    newDb.append(editItem.getPage().getURL(), editItem.getPage());
                    itemsWritten++;
                } else if (curInstruction == DEL_PAGE) {
                    // Ignore it.  We tried to delete an item
                    // that's not here.
                }

                // Either way, we always process the edit.
                hasEdits = edits.next(editItem);
            }

            // Now we have only preexisting items.  We just copy
            // them to the new file, in order.
            while (hasEntries && ! hasEdits) {
                newDb.append(readerKey, readerVal);
                itemsWritten++;
                hasEntries = db.next(readerKey, readerVal);
            }
        }
    }

    /***
     * The PagesByMD5Processor is used during close() time for
     * the pagesByMD5 table.  We instantiate one of these, and it
     * takes care of the entire shutdown process.
     */
    private class PagesByMD5Processor extends CloseProcessor {
        /**
         */
        PagesByMD5Processor(MapFile.Reader db, SequenceFile.Writer editWriter) {
            super(PAGES_BY_MD5, db, editWriter, new SequenceFile.Sorter(fs, new PageInstruction.PageComparator(), NullWritable.class), null, Page.class, NullWritable.class);
        }

        /**
         */
        void mergeEdits(MapFile.Reader db, SequenceFile.Reader sortedEdits, MapFile.Writer newDb) throws IOException {
            // Create the keys and vals
            Page readerItem = new Page();
            PageInstruction editItem = new PageInstruction();

            // For computing the GC list
            Page deletedItem = new Page(), lastItem = new Page();
            boolean justDeletedItem = false;
            boolean newReaderItem = false;
            int itemRepeats = 0;

            // Read the first items from both streams
            boolean hasEntries = db.next(readerItem, NullWritable.get());
            boolean hasEdits = sortedEdits.next(editItem, NullWritable.get());
            if (hasEntries) {
                // The first thing we read should become
                // the "previous key".  We need this for
                // garbage collection.
                outBuf.reset();
                readerItem.write(outBuf);
                inBuf.reset(outBuf.getData(), outBuf.getLength());
                lastItem.readFields(inBuf);
                itemRepeats = 0;
            }

            // As long we have both edits and entries, we need to
            // interleave them.
            while (hasEdits && hasEntries) {
                int comparison = readerItem.compareTo(editItem.getPage());
                int curInstruction = editItem.getInstruction();

                //
                // OK!  Now perform operations
                //
                if (curInstruction == ADD_PAGE) {
                    if (comparison < 0) {
                        // Write readerItem, just passing it along.
                        // Don't process the edit yet.
                        newDb.append(readerItem, NullWritable.get());
                        itemsWritten++;
                        hasEntries = db.next(readerItem, NullWritable.get());
                        newReaderItem = true;
                    } else if (comparison == 0) {
                        //
                        // This is a "replacing ADD", which is generated
                        // by the above-sequence.  We should skip over the
                        // existing item, and add the new one instead.
                        //
                        // Note that by this point, the new version of the
                        // Page from the edit sequence is guaranteed to
                        // have the correct score.  We make sure of it in
                        // the mergeEdits() for PagesByURLProcessor.
                        //
                        newDb.append(editItem.getPage(), NullWritable.get());
                        itemsWritten++;
                        hasEntries = db.next(readerItem, NullWritable.get());
                        newReaderItem = true;
                        hasEdits = sortedEdits.next(editItem, NullWritable.get());
                    } else if (comparison > 0) {
                        // Write the edit item.  We've inserted an item
                        // that comes before any others.
                        newDb.append(editItem.getPage(), NullWritable.get());
                        itemsWritten++;
                        hasEdits = sortedEdits.next(editItem, NullWritable.get());
                    }
                } else if (curInstruction == ADD_PAGE_IFN_PRESENT) {
                    throw new IOException("Should never process ADD_PAGE_IFN_PRESENT for the index:  " + editItem);
                } else if (curInstruction == DEL_PAGE) {
                    if (comparison < 0) {
                        // Write the readerKey, just passing it along.
                        // Don't process the edit yet.
                        newDb.append(readerItem, NullWritable.get());
                        itemsWritten++;
                        hasEntries = db.next(readerItem, NullWritable.get());
                        newReaderItem = true;
                    } else if (comparison == 0) {
                        // Delete it!  Remember only one entry can
                        // be deleted at a time!
                        //
                        // "Delete" the entry by skipping over the reader
                        // item.  We move onto the next item in the existing
                        // index, as well as the next edit instruction.
                        hasEntries = db.next(readerItem, NullWritable.get());
                        newReaderItem = true;
                        hasEdits = sortedEdits.next(editItem, NullWritable.get());

                        // We need to set this flag for GC'ing.
                        justDeletedItem = true;
                    } else if (comparison > 0) {
                        // This should never happen!  We should only be
                        // deleting items that actually appear!
                        throw new IOException("An unapplicable DEL_PAGE should never appear during index-merge: " + editItem);
                    }
                }

                // GARBAGE COLLECTION
                // We want to detect when we have deleted the 
                // last MD5 of a certain value.  We can have 
                // multiple MD5s in the same index, as long as
                // they have different URLs.  When the last MD5
                // is deleted, we want to know so we can modify
                // the LinkDB.
                if (newReaderItem) {
                    // If we have a different readerItem which is just
                    // the same as our last one, then we know it's a 
                    // repeat!
                    if (hasEntries && readerItem.getMD5().compareTo(lastItem.getMD5()) == 0) {
                        itemRepeats++;
                    } else {
                        // The current readerItem and the lastItem
                        // MD5s are not equal.
                        //
                        // If the last item was deleted, AND if the
                        // deleted item is not a repeat of the current item,
                        // then that MD5 should be garbage collected.
                        if (justDeletedItem && itemRepeats == 0) {
                            deleteLink(lastItem.getMD5());
                        }

                        // The current readerItem is the new "last key".
                        outBuf.reset();
                        readerItem.write(outBuf);
                        inBuf.reset(outBuf.getData(), outBuf.getLength());
                        lastItem.readFields(inBuf);
                        itemRepeats = 0;
                    }
                    // Clear "new-reader-item" bit
                    newReaderItem = false;
                }
                // Clear "last-deleted" bit
                justDeletedItem = false;
            }
        
            // Now we have only edits.  No more preexisting items!
            while (! hasEntries && hasEdits) {
                int curInstruction = editItem.getInstruction();
                if (curInstruction == ADD_PAGE) {
                    // Just write down the new page!
                    newDb.append(editItem.getPage(), NullWritable.get());
                    itemsWritten++;
                } else if (curInstruction == ADD_PAGE_IFN_PRESENT) {
                    throw new IOException("Should never process ADD_PAGE_IFN_PRESENT for the index:  " + editItem);
                } else if (curInstruction == DEL_PAGE) {
                    // This should never happen!  We should only be
                    // deleting items that actually appear!
                    throw new IOException("An unapplicable DEL_PAGE should never appear during index-merge: " + editItem);
                }
                hasEdits = sortedEdits.next(editItem, NullWritable.get());
            }

            // Now we have only preexisting items.  We just copy them
            // to the new file, in order
            while (hasEntries && ! hasEdits) {
                // Simply copy through the remaining database items
                newDb.append(readerItem, NullWritable.get());
                itemsWritten++;
                hasEntries = db.next(readerItem, NullWritable.get());
                newReaderItem = true;
            }
        }
    }

    /**
     * The LinksByMD5Processor is used during close() for
     * the pagesByMD5 table.  It processes all the edits to
     * this table, and also generates edits for the linksByURL
     * table.
     */
    private class LinksByMD5Processor extends CloseProcessor {
        SequenceFile.Writer futureEdits;

        /**
         */
        public LinksByMD5Processor(MapFile.Reader db, SequenceFile.Writer editWriter, SequenceFile.Writer futureEdits) {
            super(LINKS_BY_MD5, db, editWriter, new SequenceFile.Sorter(fs, new LinkInstruction.MD5Comparator(), NullWritable.class), new Link.MD5Comparator(), Link.class, NullWritable.class);
            this.futureEdits = futureEdits;
        }

        /**
         * Merges edits into the md5-driven link table.  Also generates
         * edit sequence to apply to the URL-driven table.
         */
        void mergeEdits(MapFile.Reader db, SequenceFile.Reader sortedEdits, MapFile.Writer newDb) throws IOException {
            WritableComparator comparator = new Link.MD5Comparator();
            DeduplicatingLinkSequenceReader edits = new DeduplicatingLinkSequenceReader(sortedEdits);

            // Create the keys and vals we'll use
            LinkInstruction editItem = new LinkInstruction();
            Link readerItem = new Link();

            // Read the first items from both streams
            boolean hasEntries = db.next(readerItem, NullWritable.get());
            boolean hasEdits = edits.next(editItem);

            // As long as we have both edits and entries to process,
            // we need to interleave them
            while (hasEntries && hasEdits) {
                int curInstruction = editItem.getInstruction();

                // Perform operations
                if (curInstruction == ADD_LINK) {
                    // When we add a link, we may replace a previous
                    //   link with identical URL and MD5 values.  The 
                    //   MD5FirstComparator will use both values.
                    //
                    int comparison = comparator.compare(readerItem, editItem.getLink());

                    if (comparison < 0) {
                        // Write the readerKey, just passing it along.
                        //   Don't process the edit yet.
                        newDb.append(readerItem, NullWritable.get());
                        itemsWritten++;
                        hasEntries = db.next(readerItem, NullWritable.get());
                    } else if (comparison == 0) {
                        // 1.  Write down the item for table-edits
                        if (futureEdits != null) {
                            linksByURLEdits++;
                            liwriter.appendInstructionInfo(futureEdits, editItem.getLink(), ADD_LINK, NullWritable.get());
                        }

                        // 2.  Write the new item, "replacing" the old one.
                        //    We move to the next edit instruction and move
                        //    past the replaced db entry.
                        newDb.append(editItem.getLink(), NullWritable.get());
                        itemsWritten++;
                        hasEntries = db.next(readerItem, NullWritable.get());
                        hasEdits = edits.next(editItem);
                    } else if (comparison > 0) {
                        // 1.  Write down the item for table-edits
                        if (futureEdits != null) {
                            linksByURLEdits++;
                            liwriter.appendInstructionInfo(futureEdits, editItem.getLink(), ADD_LINK, NullWritable.get());
                        }

                        // 2.  Write the new item.  We stay at the current
                        //     db entry.
                        newDb.append(editItem.getLink(), NullWritable.get());
                        itemsWritten++;
                        hasEdits = edits.next(editItem);
                    }
                } else if ((curInstruction == DEL_LINK) ||
                           (curInstruction == DEL_SINGLE_LINK)) {
                    // When we delete a link, we might delete many
                    //   at once!  We are interested only in the MD5
                    //   here.  If there are entries with identical MD5
                    //   values, but different URLs, we get rid of them
                    //   all.
                    int comparison = 0;
                    if (curInstruction == DEL_LINK) {
                        comparison = readerItem.getFromID().compareTo(editItem.getLink().getFromID());
                    } else {
                        comparison = readerItem.md5Compare(editItem.getLink());
                    }

                    if (comparison < 0) {
                        // Write the readerKey, just passing it along.
                        //   Don't process the edit yet.
                        newDb.append(readerItem, NullWritable.get());
                        itemsWritten++;
                        hasEntries = db.next(readerItem, NullWritable.get());
                    } else if (comparison == 0) {
                        // Delete it (or them!)
                        // 1. Write the full instruction for the next
                        //    delete-stage.  That includes the read-in
                        //    value
                        // 2. "Delete" the entry by skipping the
                        //    readerKey.  We DO NOT go to the next edit 
                        //    instruction!  There might still be more 
                        //    entries in the database to which we should
                        //    apply this delete-edit.
                        //
                        // Step 1.  Write entry for future table-edits
                        if (futureEdits != null) {
                            linksByURLEdits++;
                            liwriter.appendInstructionInfo(futureEdits, readerItem, DEL_LINK, NullWritable.get());
                        }

                        // Step 2.
                        // We might want to delete multiple MD5s with
                        // a single delete() operation, so keep this
                        // edit instruction around
                        hasEntries = db.next(readerItem, NullWritable.get());
                        if (curInstruction == DEL_SINGLE_LINK) {
                            hasEdits = edits.next(editItem);
                        }
                    } else if (comparison > 0) {
                        // Ignore, move on to next instruction
                        hasEdits = edits.next(editItem);
                    }
                }
            }

            // Now we have only edits.  No more preexisting items!
            while (! hasEntries && hasEdits) {
                int curInstruction = editItem.getInstruction();

                if (curInstruction == ADD_LINK) {
                    // 1.  Write down the item for future table-edits
                    if (futureEdits != null) {
                        linksByURLEdits++;
                        liwriter.appendInstructionInfo(futureEdits, editItem.getLink(), ADD_LINK, NullWritable.get());
                    }

                    // 2.  Just add the item from the edit list
                    newDb.append(editItem.getLink(), NullWritable.get());
                    itemsWritten++;
                } else if (curInstruction == DEL_LINK) {
                    // Ignore operation
                }
                // Move on to next edit
                hasEdits = edits.next(editItem);
            }

            // Now we have only preexisting items.  Just copy them
            // to the new file, in order.
            while (hasEntries && ! hasEdits) {
                newDb.append(readerItem, NullWritable.get());
                itemsWritten++;
                hasEntries = db.next(readerItem, NullWritable.get());
            }
        }
    }

    /**
     * This class helps the LinksByURLProcessor test a list of
     * Page objects, sorted by URL, for outlink-counts.  We query
     * this class with a series of questions, based on Links sorted
     * by target URL.
     */
    private class TargetTester {
        MapFile.Reader pagedb;
        boolean hasPage = false;
        UTF8 pageURL = null;
        Page page = null;

        /**
         */
        public TargetTester(MapFile.Reader pagedb) throws IOException {
            this.pagedb = pagedb;
            this.pageURL = new UTF8();
            this.page = new Page();
            this.hasPage = pagedb.next(pageURL, page);
        }

        /**
         * Match the given URL against the sorted series of Page URLs.
         */
        public int hasOutlinks(UTF8 curURL) throws IOException {
            int returnCode = NO_OUTLINKS;
            int comparison = pageURL.compareTo(curURL);

            while (hasPage && comparison < 0) {
                hasPage = pagedb.next(pageURL, page);
                if (hasPage) {
                    comparison = pageURL.compareTo(curURL);
                }
            }

            if (hasPage) {
                if (comparison == 0) {
                    returnCode = (page.getNumOutlinks() > 0) ? HAS_OUTLINKS : NO_OUTLINKS;
                } else if (comparison > 0) {
                    //
                    // This situation indicates that the Link's 
                    // target page has been deleted, probably
                    // because we repeatedly failed to fetch the URL.
                    // So, we should delete the Link.
                    //
                    returnCode = LINK_INVALID;
                }
            }
            return returnCode;
        }

        /**
         */
        public void close() throws IOException {
            pagedb.close();
        }
    }

    /**
     * Closes down and merges changes to the URL-driven link
     * table.  This does nothing fancy, and propagates nothing
     * to a further stage.  There is no next stage!
     */
    private class LinksByURLProcessor extends CloseProcessor {
        MapFile.Reader pageDb;
        SequenceFile.Writer futureEdits;

        /**
         */
        public LinksByURLProcessor(MapFile.Reader db, SequenceFile.Writer editWriter, MapFile.Reader pageDb, SequenceFile.Writer futureEdits) {
            super(LINKS_BY_URL, db, editWriter, new SequenceFile.Sorter(fs, new LinkInstruction.UrlComparator(), NullWritable.class), new Link.UrlComparator(), Link.class, NullWritable.class);
            this.pageDb = pageDb;
            this.futureEdits = futureEdits;
        }

        /**
         */
        public long closeDown(File workingDir, File outputDir, long numEdits) throws IOException {
            long result = super.closeDown(workingDir, outputDir, numEdits);
            //pageDb.close();
            return result;
        }

        /**
         * Merge the existing db with the edit-stream into a brand-new file.
         */
        void mergeEdits(MapFile.Reader db, SequenceFile.Reader sortedEdits, MapFile.Writer newDb) throws IOException {
            WritableComparator comparator = new Link.UrlComparator();

            // Create the keys and vals we'll use
            LinkInstruction editItem = new LinkInstruction();
            Link readerItem = new Link();
        
            // Read the first items from both streams
            boolean hasEntries = db.next(readerItem, NullWritable.get());
            boolean hasEdits = sortedEdits.next(editItem, NullWritable.get());
            TargetTester targetTester = new TargetTester(pageDb);

            // As long as we have both edits and entries to process,
            // we need to interleave them
            while (hasEntries && hasEdits) {
                int curInstruction = editItem.getInstruction();

                if (curInstruction == ADD_LINK) {
                    //  When we add a link, we may replace a previous
                    //    link with identical URL and MD5 values.  Our
                    //    comparator will test both
                    //
                    int comparison = comparator.compare(readerItem, editItem.getLink());

                    if (comparison < 0) {
                        // Write the readerKey, just passing it along.
                        // Don't process the edit yet.
                        int linkTest = targetTester.hasOutlinks(readerItem.getURL());

                        if (linkTest == LINK_INVALID) {
                            liwriter.appendInstructionInfo(futureEdits, readerItem, DEL_SINGLE_LINK, NullWritable.get());
                            targetOutlinkEdits++;
                        } else {
                            boolean oldOutlinkStatus = readerItem.targetHasOutlink();
                            boolean newOutlinkStatus = (linkTest == HAS_OUTLINKS);
                            // Do the conditional so we minimize unnecessary 
                            // mod-writes.
                            if (oldOutlinkStatus != newOutlinkStatus) {
                                readerItem.setTargetHasOutlink(newOutlinkStatus);
                                liwriter.appendInstructionInfo(futureEdits, readerItem, ADD_LINK, NullWritable.get());
                                targetOutlinkEdits++;
                            }
                            newDb.append(readerItem, NullWritable.get());
                            itemsWritten++;
                        }
                        hasEntries = db.next(readerItem, NullWritable.get());
                    } else if (comparison == 0) {
                        // Write the new item, "replacing" the old one.
                        // We move to the next edit instruction and move
                        //    past the replaced db entry.
                        Link editLink = editItem.getLink();
                        int linkTest = targetTester.hasOutlinks(editLink.getURL());

                        // Delete the edit/readerItem from the other table if it's
                        // found to be invalid.
                        if (linkTest == LINK_INVALID) {
                            liwriter.appendInstructionInfo(futureEdits, editLink, DEL_SINGLE_LINK, NullWritable.get());
                        } else {
                            editLink.setTargetHasOutlink(linkTest == HAS_OUTLINKS);
                            liwriter.appendInstructionInfo(futureEdits, editLink, ADD_LINK, NullWritable.get());

                            newDb.append(editLink, NullWritable.get());
                            itemsWritten++;
                        }
                        targetOutlinkEdits++;

                        hasEntries = db.next(readerItem, NullWritable.get());
                        hasEdits = sortedEdits.next(editItem, NullWritable.get());
                    } else if (comparison > 0) {
                        // Write the new item.  We stay at the current
                        // db entry.
                        Link editLink = editItem.getLink();
                        int linkTest = targetTester.hasOutlinks(editLink.getURL());

                        // Delete the edit from the other table if it's invalid
                        if (linkTest == LINK_INVALID) {
                            liwriter.appendInstructionInfo(futureEdits, editLink, DEL_SINGLE_LINK, NullWritable.get());
                        } else {
                            editLink.setTargetHasOutlink(linkTest == HAS_OUTLINKS);
                            liwriter.appendInstructionInfo(futureEdits, editLink, ADD_LINK, NullWritable.get());
                            newDb.append(editLink, NullWritable.get());
                            itemsWritten++;
                        }
                        targetOutlinkEdits++;

                        hasEdits = sortedEdits.next(editItem, NullWritable.get());
                    }
                } else if (curInstruction == DEL_LINK) {
                    // When we delete a link, we do it by MD5 and apply
                    //   it to the index first.  A single delete instruction
                    //   may remove many items in the db, during the earlier
                    //   processing.  However, unlike the index-processing stage,
                    //   here we can expect a new DEL instruction for every 
                    //   item that we remove from the db.
                    //
                    int comparison = comparator.compare(readerItem, editItem.getLink());

                    if (comparison < 0) {
                        // Write readerKey, just passing it along.  Don't
                        //   process the edit yet.
                        int linkTest = targetTester.hasOutlinks(readerItem.getURL());

                        // Delete the reader item if it's found to be invalid
                        if (linkTest == LINK_INVALID) {
                            liwriter.appendInstructionInfo(futureEdits, readerItem, DEL_SINGLE_LINK, NullWritable.get());
                        } else {
                            readerItem.setTargetHasOutlink(linkTest == HAS_OUTLINKS);
                            liwriter.appendInstructionInfo(futureEdits, readerItem, ADD_LINK, NullWritable.get());
                            newDb.append(readerItem, NullWritable.get());
                            itemsWritten++;
                        }
                        targetOutlinkEdits++;

                        hasEntries = db.next(readerItem, NullWritable.get());
                    } else if (comparison == 0) {
                        // "Delete" the item by passing by the readerKey.
                        // We want a new entry, as well as the next instruction
                        // to process.
                        hasEntries = db.next(readerItem, NullWritable.get());
                        hasEdits = sortedEdits.next(editItem, NullWritable.get());
                    } else if (comparison > 0) {
                        // Ignore, move on to next instruction
                        hasEdits = sortedEdits.next(editItem, NullWritable.get());
                    }
                }
            }

            // Now we have only edits.  No more preexisting items!
            while (! hasEntries && hasEdits) {
                int curInstruction = editItem.getInstruction();

                if (curInstruction == ADD_LINK) {
                    //
                    // Add the item from the edit list.
                    //

                    //
                    // Make sure the outlinks flag is set properly.
                    //
                    Link editLink = editItem.getLink();
                    int linkTest = targetTester.hasOutlinks(editLink.getURL());
                    if (linkTest == LINK_INVALID) {
                        liwriter.appendInstructionInfo(futureEdits, editLink, DEL_SINGLE_LINK, NullWritable.get());
                    } else {
                        editLink.setTargetHasOutlink(linkTest == HAS_OUTLINKS);
                        liwriter.appendInstructionInfo(futureEdits, editLink, ADD_LINK, NullWritable.get());
                        newDb.append(editLink, NullWritable.get());
                        itemsWritten++;
                    }
                    targetOutlinkEdits++;
                } else if (curInstruction == DEL_LINK) {
                    // Ignore operation
                }
                // Move on to next edit
                hasEdits = sortedEdits.next(editItem, NullWritable.get());
            }

            // Now we have only preexisting items.  Just copy them
            // to the new file, in order.
            while (hasEntries && ! hasEdits) {
                //
                // Simply copy the remaining database items.
                //

                //
                // First, make sure the 'outlinks' flag is set properly.
                //
                int linkTest = targetTester.hasOutlinks(readerItem.getURL());
                if (linkTest == LINK_INVALID) {
                    liwriter.appendInstructionInfo(futureEdits, readerItem, DEL_SINGLE_LINK, NullWritable.get());
                    targetOutlinkEdits++;
                } else {
                    boolean oldOutlinkStatus = readerItem.targetHasOutlink();
                    boolean newOutlinkStatus = (linkTest == HAS_OUTLINKS);
                    if (oldOutlinkStatus != newOutlinkStatus) {
                        readerItem.setTargetHasOutlink(newOutlinkStatus);
                        liwriter.appendInstructionInfo(futureEdits, readerItem, ADD_LINK, NullWritable.get());
                        targetOutlinkEdits++;
                    }

                    // Now copy the object
                    newDb.append(readerItem, NullWritable.get());
                    itemsWritten++;
                }

                // Move on to next
                hasEntries = db.next(readerItem, NullWritable.get());
            }

            targetTester.close();
        }
    }

    /**
     * Create the WebDB for the first time.
     */
    public static void createWebDB(NutchFileSystem nfs, File dbDir) throws IOException {
        WebDBWriter starter = new WebDBWriter(nfs, dbDir, true);
        starter.close();
    }
    
    boolean haveEdits = false;
    NutchFileSystem fs = null;
    File dbDir, dbFile, oldDbFile, newDbFile, tmp;
    MapFile.Reader pagesByURL, pagesByMD5, linksByURL, linksByMD5;
    SequenceFile.Writer pagesByURLWriter, pagesByMD5Writer, linksByURLWriter, linksByMD5Writer;
    long pagesByURLEdits = 0, pagesByMD5Edits = 0, linksByURLEdits = 0, linksByMD5Edits = 0, targetOutlinkEdits = 0;
    PageInstructionWriter piwriter = new PageInstructionWriter();
    LinkInstructionWriter liwriter = new LinkInstructionWriter();
    DataInputBuffer inBuf = new DataInputBuffer();
    DataOutputBuffer outBuf = new DataOutputBuffer();

    /**
     * Create a WebDBWriter.
     */
    public WebDBWriter(NutchFileSystem fs, File dbDir) throws IOException {
        this(fs, dbDir, false);
    }

    /**
     * Private constructor, so we can either open or create the db files.
     */
    private WebDBWriter(NutchFileSystem fs, File dbDir, boolean create) throws IOException {
        this.fs = fs;
        this.dbDir = dbDir;
        this.dbFile = new File(dbDir, "webdb");
        this.oldDbFile = new File(dbDir, "webdb.old");
        this.newDbFile = new File(dbDir, "webdb.new");
        this.tmp = new File(newDbFile, "tmp");

        if ((! fs.exists(dbDir)) && create) {
            fs.mkdirs(dbDir);
        }
        if (! fs.exists(dbDir) || ! fs.isDirectory(dbDir)) {
            throw new IOException("Database " + dbDir + " is not a directory.");
        }

        // Lock the writeLock immediately.
        fs.lock(new File(dbDir, "dbwritelock"), false);

        // Resolve any partial-state dirs from the last run.
        if (fs.exists(oldDbFile)) {
            if (fs.exists(dbFile)) {
                throw new IOException("Impossible condition: directories " + oldDbFile + " and " + dbFile + " cannot exist simultaneously");
            }
            if (fs.exists(newDbFile)) {
                fs.rename(newDbFile, dbFile);
            }
            FileUtil.fullyDelete(fs, oldDbFile);
        } else if (fs.exists(newDbFile)) {
            FileUtil.fullyDelete(fs, newDbFile);
        }

        // Create the directory, if necessary
        if ((! fs.exists(dbFile)) && create) {
            fs.mkdirs(dbFile);
        }

        // Delete any partial edits from last time.
        if (fs.exists(tmp)) {
            FileUtil.fullyDelete(fs, tmp);
        }
        fs.mkdirs(tmp);

        // Create the file names we need
        if (create) {
            new MapFile.Writer(fs, new File(dbFile, PAGES_BY_URL).getPath(), new UTF8.Comparator(), Page.class).close();
            new MapFile.Writer(fs, new File(dbFile, PAGES_BY_MD5).getPath(), new Page.Comparator(), NullWritable.class).close();
            new MapFile.Writer(fs, new File(dbFile, LINKS_BY_URL).getPath(), new Link.UrlComparator(), NullWritable.class).close();
            new MapFile.Writer(fs, new File(dbFile, LINKS_BY_MD5).getPath(), new Link.MD5Comparator(), NullWritable.class).close();
        }

        // Create the Readers for those files
        this.pagesByURL = new MapFile.Reader(fs, new File(dbFile, PAGES_BY_URL).getPath(), new UTF8.Comparator());
        this.pagesByMD5 = new MapFile.Reader(fs, new File(dbFile, PAGES_BY_MD5).getPath(), new Page.Comparator());
        this.linksByURL = new MapFile.Reader(fs, new File(dbFile, LINKS_BY_URL).getPath(), new Link.UrlComparator());
        this.linksByMD5 = new MapFile.Reader(fs, new File(dbFile, LINKS_BY_MD5).getPath(), new Link.MD5Comparator());

        // Create writers for new edit-files.  We write changes
        // into these files, then apply them to the db upon close().
        pagesByURLWriter = new SequenceFile.Writer(fs, new File(tmp, PAGES_BY_URL + ".out").getPath(), PageInstruction.class, NullWritable.class);
        pagesByMD5Writer = new SequenceFile.Writer(fs, new File(tmp, PAGES_BY_MD5 + ".out").getPath(), PageInstruction.class, NullWritable.class);
        linksByURLWriter = new SequenceFile.Writer(fs, new File(tmp, LINKS_BY_URL + ".out").getPath(), LinkInstruction.class, NullWritable.class);
        linksByMD5Writer = new SequenceFile.Writer(fs, new File(tmp, LINKS_BY_MD5 + ".out").getPath(), LinkInstruction.class, NullWritable.class);
    }

    /**
     * Shutdown
     */
    public synchronized void close() throws IOException {
        if (haveEdits) {
            fs.mkdirs(newDbFile);

            // Process the 4 tables:
            // 1. pagesByURL
            // 2. pagesByMD5
            // 3. linksByMD5
            // 4. linksByURL

            // 1. Process pagesByURL.  Processing this stream will
            // generate a number of edits for the pagesByMD5 step.
            //
            CloseProcessor pagesByURLProcessor = new PagesByURLProcessor(pagesByURL, pagesByURLWriter, pagesByMD5Writer);
            long numPBUItems = pagesByURLProcessor.closeDown(tmp, newDbFile, pagesByURLEdits);

            //
            // 2.  Process the pagesByMD5 edit stream.  This will
            // make calls to deleteLink(), which are processed later.
            //
            CloseProcessor pagesByMD5Processor = new PagesByMD5Processor(pagesByMD5, pagesByMD5Writer);
            long numPBMItems = pagesByMD5Processor.closeDown(tmp, newDbFile, pagesByMD5Edits);

            //
            // 3. Process the linksByMD5 edit stream first.  This
            // will generate a number of edits for the linksByURL
            // stream.  This also processes the calls to deleteLink()
            // that may have been invoked as part of the above call
            // to process pagesByMD5.
            CloseProcessor linksByMD5Processor = new LinksByMD5Processor(linksByMD5, linksByMD5Writer, linksByURLWriter);
            long numLBMItems = linksByMD5Processor.closeDown(tmp, newDbFile, linksByMD5Edits);

            //
            // 4. Process the linksByURL edit stream.  This will also
            // read through the sorted PagesByURL file, and modify
            // the Links so that they indicated whether the target
            // Page has any outlinks or not.
            //
            SequenceFile.Writer targetOutlinkEditsWriter = new SequenceFile.Writer(fs, new File(tmp, LINKS_BY_MD5 + ".out").getPath(), LinkInstruction.class, NullWritable.class);
            CloseProcessor linksByURLProcessor = new LinksByURLProcessor(linksByURL, linksByURLWriter, new MapFile.Reader(fs, new File(newDbFile, PAGES_BY_URL).getPath(), new UTF8.Comparator()), targetOutlinkEditsWriter);
            long numLBUItems = linksByURLProcessor.closeDown(tmp, newDbFile, linksByURLEdits);

            //
            // If the number of linksByURL processed is zero, then
            // there's no reason to do all of the following with
            // a 2nd pass through linksByMD5.
            //
            if (numLBUItems == 0) {
                targetOutlinkEditsWriter.close();

                //
                // Need to load in the previous number of links, so we
                // don't write over with the wrong value.
                //
                File stats = new File(dbFile, STATS_FILE);
                if (fs.exists(stats)) {
                    DataInputStream in = new DataInputStream(fs.open(stats));
                    try {
                        in.read();                   // version
                        in.readLong();               // previous num of pages
                        numLBMItems = in.readLong(); // previous num of links
                    } finally {
                        in.close();
                    }
                }
            } else {
                //
                // 5. Step 4 did several things to the LinksByURL db.
                // First, it implemented all the changes generated
                // by instructions from LinksByMD5Processor.  Second,
                // it made lots of calls to setTargetHasOutlink.  This
                // changes the content of the Link objects.
                //
                // So now we need to reconstruct the LinksByMD5
                // list, using the Links we created in step #4.
                //

                File stageTwoDbFile = new File(newDbFile, "stage2.subdir");
                fs.mkdirs(stageTwoDbFile);

                MapFile.Reader linksByMD5ForStageTwo = new MapFile.Reader(fs, new File(newDbFile, LINKS_BY_MD5).getPath(), new Link.MD5Comparator());
                CloseProcessor linksByMD5StageTwoProcessor = new LinksByMD5Processor(linksByMD5ForStageTwo, targetOutlinkEditsWriter, null);
                numLBMItems = linksByMD5StageTwoProcessor.closeDown(tmp, stageTwoDbFile, targetOutlinkEdits);

                //
                // 6. Now move the Stage2 LinksByMD5 file up to replace
                // the one at the primary level
                //
                linksByMD5ForStageTwo.close();
                File stageOneLinksByMD5 = new File(newDbFile, LINKS_BY_MD5);
                fs.delete(stageOneLinksByMD5);
                fs.rename(new File(stageTwoDbFile, LINKS_BY_MD5), stageOneLinksByMD5);
                fs.delete(stageTwoDbFile);
            }

            //
            // 7. Finally, write out the total num of pages and links
            //
            File stats = new File(newDbFile, STATS_FILE);
            DataOutputStream out = new DataOutputStream(fs.create(stats));
            try {
                //
                // These counts are guaranteed to be correct; they're
                // based on the counts made during processing of primary-key
                // edits.  Pages are always counted by URL first, and only
                // subsequently by MD5 if there are any edits to make.  Links
                // are always counted by MD5 first, and only by URL subsequently
                // and conditionally.  
                //
                // If there are a bunch of edits that result in no modifications
                // to the db, the two sets of counts (one for URL, one for
                // MD5) could become out of sync.  So we use the ones that
                // are sure to be accurate.
                //
                out.write(CUR_VERSION);
                out.writeLong(numPBUItems);
                out.writeLong(numLBMItems);
            } finally {
                out.close();
            }
        } else {
            pagesByURLWriter.close();
            pagesByMD5Writer.close();
            linksByMD5Writer.close();
            linksByURLWriter.close();
        }

        // Close down the db-readers
        pagesByURL.close();
        pagesByMD5.close();
        linksByMD5.close();
        linksByURL.close();

        // Delete the edits directory.
        fs.delete(tmp);

        // Before we can move the newdb into place over the
        // old one, we need to make sure there are no processes
        // reading the old db.  This obtains an exclusive lock
        // on the directory that holds our dbs (old and new).
        fs.lock(new File(dbDir, "dbreadlock"), false);

        // We're done!  Now we rename the directories and
        // all is well.
        if (haveEdits) {
            fs.rename(dbFile, oldDbFile);
            fs.rename(newDbFile, dbFile);
            FileUtil.fullyDelete(fs, oldDbFile);
        } else {
            // Sometimes the "newdb" is created as a side-effect
            // of creating the tmp dir, even when there are no edits.
            // Get rid of it.
            FileUtil.fullyDelete(fs, newDbFile);
        }

        // release the readlock
        fs.release(new File(dbDir, "dbreadlock"));

        // release the writelock
        fs.release(new File(dbDir, "dbwritelock"));
    }

    /////////////////////
    // Methods for adding, and managing, db operations
    ////////////////////

    /**
     * Add a page to the page database
     */
    public synchronized void addPage(Page page) throws IOException {
        // The 2nd (byMD5) part is handled during processing of the 1st.
        haveEdits = true;
        pagesByURLEdits++;
        piwriter.appendInstructionInfo(pagesByURLWriter, page, ADD_PAGE, NullWritable.get());
    }

    /**
     * Add a page to the page database, with a brand-new score
     */
    public synchronized void addPageWithScore(Page page) throws IOException {
        // The 2nd (byMD5) part is handled during processing of the 1st.
        haveEdits = true;
        pagesByURLEdits++;
        piwriter.appendInstructionInfo(pagesByURLWriter, page, ADD_PAGE_WITH_SCORE, NullWritable.get());
    }

    /**
     * Don't replace the one in the database, if there is one.
     */
    public synchronized void addPageIfNotPresent(Page page) throws IOException {
        // The 2nd (index) part is handled during processing of the 1st.
        haveEdits = true;
        pagesByURLEdits++;
        piwriter.appendInstructionInfo(pagesByURLWriter, page, ADD_PAGE_IFN_PRESENT, NullWritable.get());        
    }

    /**
     * Don't replace the one in the database, if there is one.
     *
     * If we do insert the new Page, then we should also insert
     * the given Link object.
     */
    public synchronized void addPageIfNotPresent(Page page, Link link) throws IOException {
        // The 2nd (index) part is handled during processing of the 1st.
        haveEdits = true;
        pagesByURLEdits++;
        piwriter.appendInstructionInfo(pagesByURLWriter, page, link, ADD_PAGE_IFN_PRESENT, NullWritable.get());        
    }

    /**
     * Remove a page from the page database.
     */
    public synchronized void deletePage(String url) throws IOException {
        // The 2nd (index) part is handled during processing of the 1st.
        haveEdits = true;
        Page p = new Page();
        p.setURL(url);
        pagesByURLEdits++;        
        piwriter.appendInstructionInfo(pagesByURLWriter, p, DEL_PAGE, NullWritable.get());
    }

    /**
     * Add a link to the link database
     */
    public synchronized void addLink(Link lr) throws IOException {
        haveEdits = true;
        linksByMD5Edits++;
        liwriter.appendInstructionInfo(linksByMD5Writer, lr, ADD_LINK, NullWritable.get());
    }

    /**
     * Remove links with the given MD5 from the db.
     */
    public synchronized void deleteLink(MD5Hash md5) throws IOException {
        haveEdits = true;
        linksByMD5Edits++;
        liwriter.appendInstructionInfo(linksByMD5Writer, new Link(md5, 0, "", ""), DEL_LINK, NullWritable.get());
    }

    /**
     * The WebDBWriter.main() provides some handy methods for
     * testing the WebDB.
     */
    public static void main(String argv[]) throws FileNotFoundException, IOException {
        if (argv.length < 2) {
            System.out.println("Usage: java org.apache.nutch.db.WebDBWriter (-local | -ndfs <namenode:port>) <db> [-create] | [-addpage id url] | [-addpageifnp id url] | [-deletepage url] | [-addlink fromID url] | [-deletelink fromID]");
            return;
        }

        int i = 0;
        NutchFileSystem nfs = NutchFileSystem.parseArgs(argv, i);
        File dbDir = new File(argv[i++]);
        try {
            if ("-create".equals(argv[i])) {
                i++;
                WebDBWriter.createWebDB(nfs, dbDir);
                System.out.println("Created webdb at " + nfs + ":" + dbDir);
            } else if ("-addpage".equals(argv[i])) {
                i++;
                MD5Hash md5 = new MD5Hash(argv[i++]);
                String url = argv[i++];

                WebDBWriter writer = new WebDBWriter(nfs, dbDir);
                try {
                    Page page = new Page(url, md5);
                    writer.addPageWithScore(page);
                    System.out.println("Added page (with score): " + page);
                } finally {
                    writer.close();
                } 
            } else if ("-addpageifnp".equals(argv[i])) {
                i++;
                MD5Hash md5 = new MD5Hash(argv[i++]);
                String url = argv[i++];

                WebDBWriter writer = new WebDBWriter(nfs, dbDir);
                try {
                    Page page = new Page(url, md5);
                    writer.addPageIfNotPresent(page);
                    System.out.println("Added page: " + page);
                } finally {
                    writer.close();
                } 
            } else if ("-deletepage".equals(argv[i])) {
                i++;
                String url = argv[i++];

                WebDBWriter writer = new WebDBWriter(nfs, dbDir);
                try {
                    writer.deletePage(url.trim());
                    System.out.println("Deleted item(s)");
                } finally {
                    writer.close();
                }
            } else if ("-addlink".equals(argv[i])) {
                i++;
                MD5Hash fromID = new MD5Hash(argv[i++]);
                String url = argv[i++];

                WebDBWriter writer = new WebDBWriter(nfs, dbDir);
                try {
                    Link link = new Link(fromID, MD5Hash.digest("randomstring.com").halfDigest(), url, "SomeRandomAnchorText_" + System.currentTimeMillis());
                    writer.addLink(link);
                    System.out.println("Added link: " + link);
                } finally {
                    writer.close();
                }
            } else if ("-deletelink".equals(argv[i])) {
                i++;
                MD5Hash fromID = new MD5Hash(argv[i++]);

                WebDBWriter writer = new WebDBWriter(nfs, dbDir);
                try {
                    writer.deleteLink(fromID);
                    System.out.println("Deleted item(s)");
                } finally {
                    writer.close();
                }
            } else {
                System.out.println("Sorry, no command with name " + argv[i]);
            }
        } finally {
            nfs.close();
        }
    }
}



