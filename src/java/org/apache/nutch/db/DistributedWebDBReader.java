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
import org.apache.nutch.pagedb.*;
import org.apache.nutch.linkdb.*;

/**********************************************
 * The WebDBReader implements all the read-only
 * parts of accessing our web database.
 * All the writing ones can be found in WebDBWriter.
 *
 * @author Mike Cafarella
 **********************************************/
public class DistributedWebDBReader implements IWebDBReader {
    static final Page[] PAGE_RECORDS = new Page[0];
    static final Link[] LINK_RECORDS = new Link[0];

    // filenames
    static final String PAGES_BY_URL = "pagesByURL";
    static final String PAGES_BY_MD5 = "pagesByMD5";
    static final String LINKS_BY_URL = "linksByURL";
    static final String LINKS_BY_MD5 = "linksByMD5";

    static final String STATS_FILE = "stats";
    static final String META_FILE = "metainfo";

    // For different enumeration types
    static final EnumCall PAGE_ENUMS = new PageEnumCall();
    static final EnumCall PAGE_MD5_ENUMS = new PageByMD5EnumCall();
    static final EnumCall LINK_ENUMS = new LinkEnumCall();    

    // Utility array for Vector conversion
    static final DBSectionReader[] STATIC_SR_ARRAY = new DBSectionReader[0];

    // Structures for multi-file db structures
    File root, dbDir;
    File globalWriteLock;
    DBSectionReader pagesByURL[], pagesByMD5[], linksByURL[], linksByMD5[];
    long totalPages = 0, totalLinks = 0;
    int numMachines = 0;

    /**
     * Open a web db reader for the named directory.
     */    
    public DistributedWebDBReader(NutchFileSystem nfs, File root) throws IOException, FileNotFoundException {
        //
        // Get the current db from the given nutchfs.  It consists
        // of a bunch of directories full of files.  
        //
        this.root = root;
        this.dbDir = new File(new File(root, "standard"), "webdb");

        //
        // Wait until the webdb is complete, by waiting till a given
        // file exists.
        //
        File dirIsComplete = new File(dbDir, "dbIsComplete");
        while (! nfs.exists(dirIsComplete)) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ie) {
            }
        }

        //
        // Obtain non-exclusive lock on the webdb's globalWriteLock 
        // so writers don't move it out from under us.
        //

        // REMIND - mjc - I think the locking here is suspect.
        /**
        this.globalWriteLock = new File("standard", new File("globalWriteLock"));
        nfs.lock(globalWriteLock, false);
        **/

        //
        // Load in how many segments we can expect
        //
        File machineInfo = new File(new File(root, "standard"), "machineinfo");
        DataInputStream in = new DataInputStream(nfs.open(machineInfo));
        try {
            in.readByte();  // version
            this.numMachines = in.readInt();
        } finally {
            in.close();
        }

        // 
        // Find all the "section" subdirs.  Each section will contain 
        // one of the 4 tables we're after.  Create one DBSectionReader 
        // object for each table in each section.
        //
        Vector pagesByURL = new Vector(), pagesByMD5 = new Vector(), linksByMD5 = new Vector(), linksByURL = new Vector();
        for (int i = 0; i < numMachines; i++) {
            // The relevant NutchFiles for each part of this db section
            File sectionDir = new File(dbDir, "dbsection." + i);
            File pagesByURLFile = new File(sectionDir, PAGES_BY_URL);
            File pagesByMD5File = new File(sectionDir, PAGES_BY_MD5);
            File linksByURLFile = new File(sectionDir, LINKS_BY_URL);
            File linksByMD5File = new File(sectionDir, LINKS_BY_MD5);

            // Create DBSectionReader object for each subtype
            pagesByURL.add(new DBSectionReader(nfs, pagesByURLFile, new UTF8.Comparator()));
            pagesByMD5.add(new DBSectionReader(nfs, pagesByMD5File, new Page.Comparator()));
            linksByURL.add(new DBSectionReader(nfs, linksByURLFile, new Link.UrlComparator()));
            linksByMD5.add(new DBSectionReader(nfs, linksByMD5File, new Link.MD5Comparator()));

            // Load in the stats file for the section
            File sectionStats = new File(sectionDir, STATS_FILE);
            in = new DataInputStream(nfs.open(sectionStats));
            try {
                in.read(); // version
                this.totalPages += in.readLong();
                this.totalLinks += in.readLong();
            } finally {
                in.close();
            }
        }

        // Put lists into array form
        this.pagesByURL = (DBSectionReader[]) pagesByURL.toArray(STATIC_SR_ARRAY);
        this.pagesByMD5 = (DBSectionReader[]) pagesByMD5.toArray(STATIC_SR_ARRAY);
        this.linksByURL = (DBSectionReader[]) linksByURL.toArray(STATIC_SR_ARRAY);
        this.linksByMD5 = (DBSectionReader[]) linksByMD5.toArray(STATIC_SR_ARRAY);
    }

    /**
     * Shutdown
     */
    public void close() throws IOException {
        for (int i = 0; i < pagesByURL.length; i++) {
            pagesByURL[i].close();
            pagesByMD5[i].close();
            linksByURL[i].close();
            linksByMD5[i].close();
        }
    }

    /**
     * How many sections (machines) there are in this distributed db.
     */
    public int numMachines() {
        return numMachines;
    }

    /**
     * Return the number of pages we're dealing with.
     */
    public long numPages() {
        return totalPages;
    }

    /**
     * Return the number of links in our db.
     */
    public long numLinks() {
        return totalLinks;
    }

    /**
     * Get Page from the pagedb with the given URL.
     */
    public Page getPage(String url) throws IOException {
        Page result = null, target = new Page();
        UTF8 searchURL = new UTF8(url);

        // Don't do linear search.  Instead, jump to the
        // chunk that will have it.
        return pagesByURL[DBKeyDivision.findURLSection(url, numMachines)].getPage(searchURL, target);
    }

    /**
     * Get all the Pages according to their content hash.
     * Since items in the pagesByMD5 DBSectionReader array will 
     * be sorted by ascending blocks of the content hash, 
     * we know the results will come in sorted order.
     */
    public Page[] getPages(MD5Hash md5) throws IOException {
        Vector resultSet = pagesByMD5[DBKeyDivision.findMD5Section(md5, numMachines)].getPages(md5);
        Page resultArray[] = new Page[resultSet.size()];
        int i = 0;
        for (Enumeration e = resultSet.elements(); e.hasMoreElements(); i++) {
            resultArray[i] = (Page) e.nextElement();
        }
        return resultArray;
    }

    /**
     * Test whether a certain piece of content is in the
     * database, but don't bother returning the Page(s) itself.
     * We need to test every DBSectionReader in pagesByMD5 until
     * we reach the end, or find a positive.
     */
    public boolean pageExists(MD5Hash md5) throws IOException {
        return pagesByMD5[DBKeyDivision.findMD5Section(md5, numMachines)].pageExists(md5);
    }

    /**
     * Iterate through all the Pages, sorted by URL.
     * We need to enumerate all the Enumerations given 
     * to us via a call to pages() for each DBSectionReader.
     */
    public Enumeration pages() throws IOException {
        return new MetaEnumerator(pagesByURL, PAGE_ENUMS);
    }

    /**
     * Iterate through all the Pages, sorted by MD5.
     * We enumerate all the DBSectionReader Enumerations,
     * just as above.
     */
    public Enumeration pagesByMD5() throws IOException {
        return new MetaEnumerator(pagesByMD5, PAGE_MD5_ENUMS);
    }

    /**
     * Get all the hyperlinks that link TO the indicated URL.
     */     
    public Link[] getLinks(UTF8 url) throws IOException {
        Vector resultSet = linksByURL[DBKeyDivision.findURLSection(url.toString(), numMachines)].getLinks(url);
        Link resultArray[] = new Link[resultSet.size()];
        int i = 0;
        for (Enumeration e = resultSet.elements(); e.hasMoreElements(); ) {
            resultArray[i++] = (Link) e.nextElement();
        }
        return resultArray;
    }

    /**
     * Grab all the links from the given MD5 hash.
     */
    public Link[] getLinks(MD5Hash md5) throws IOException {
        Vector resultSet = linksByMD5[DBKeyDivision.findMD5Section(md5, numMachines)].getLinks(md5);
        Link resultArray[] = new Link[resultSet.size()];
        int i = 0;
        for (Enumeration e = resultSet.elements(); e.hasMoreElements(); ) {
            resultArray[i++] = (Link) e.nextElement();
        }
        return resultArray;
    }

    /**
     * Return all the links, by target URL
     */
    public Enumeration links() throws IOException {
        return new MetaEnumerator(linksByURL, LINK_ENUMS);
    }

    //
    // The EnumCall class allows the creator of MetaEnumerator
    // to indicate how to get each enumeration.  Will it be pages
    // or links?
    //
    static abstract class EnumCall {
        /**
         */
        public EnumCall() {
        }

        /**
         * Subclasses override this for different kinds of MetaEnumerator
         * behavior.
         */
        public abstract Enumeration getEnumeration(DBSectionReader reader) throws IOException;
    }

    //
    // For enumerating Pages
    //
    static class PageEnumCall extends EnumCall {
        /**
         */
        public PageEnumCall() {
        }

        /**
         * Get the enum of Pages
         */
        public Enumeration getEnumeration(DBSectionReader reader) throws IOException {
            return reader.pages();
        }
    }

    //
    // For enumerating Pages
    //
    static class PageByMD5EnumCall extends EnumCall {
        /**
         */
        public PageByMD5EnumCall() {
        }

        /**
         * Get the enum of Pages
         */
        public Enumeration getEnumeration(DBSectionReader reader) throws IOException {
            return reader.pagesByMD5();
        }
    }

    //
    // For enumerating Links
    //
    static class LinkEnumCall extends EnumCall {
        /**
         */
        public LinkEnumCall() {
        }

        /**
         * Get the enum of Links
         */
        public Enumeration getEnumeration(DBSectionReader reader) throws IOException {
            return reader.links();
        }
    }

    //
    // MetaEnumerator uses the Enumerations from each
    // DBSectionReader in the passed-in DBSectionReader array.
    //
    class MetaEnumerator implements Enumeration {
        Enumeration enumerations[];
        int curEnum = 0;

        /**
         * Create all the Enumerations from the given Sections
         */
        public MetaEnumerator(DBSectionReader sections[], EnumCall enumCall) throws IOException {
            this.enumerations = new Enumeration[sections.length];

            for (int i = 0; i < enumerations.length; i++) {
                enumerations[i] = enumCall.getEnumeration(sections[i]);
            }
        }

        /**
         * Go through all the DBSectionReader items in
         * pagesByURL, until we find one that hasMoreElements.
         * Or until we hit the end.
         */
        public boolean hasMoreElements() {
            boolean result = false;

            //
            // Go through Enumerations until we find one with
            // hasMoreElements() == true.  (Or until we run out
            // of Enumerations.)
            //
            for (; curEnum < enumerations.length; curEnum++) {
                result = enumerations[curEnum].hasMoreElements();
                
                if (result) {
                    break;
                }
            }
            return result;
        }

        /**
         * Exhaust the Objects we can receive from the 
         * Enumerations array, via calls to nextElement();
         */
        public Object nextElement() {
            Object obj = null;

            //
            // Go through Enumerations until we find one with
            // a nextElement() to return.  (Or until we run out.)
            //
            for (; curEnum < enumerations.length; curEnum++) {
                if (enumerations[curEnum].hasMoreElements()) {
                    obj = enumerations[curEnum].nextElement();

                    if (obj != null) {
                        break;
                    }
                }
            }
            return obj;
        }
    }

    /**
     * The DistributedWebDBReader.main() provides some handy utility methods
     * for looking through the contents of the webdb.  Hoo-boy!
     *
     * Note this only works for a completely-NFS deployment.
     */
    public static void main(String argv[]) throws FileNotFoundException, IOException {
        if (argv.length < 2) {
            System.out.println("Usage: java org.apache.nutch.db.DistributedWebDBReader (-local | -ndfs <namenode:port>) <root> [-pageurl url] | [-pagemd5 md5] | [-dumppageurl] | [-dumppagemd5] | [-toppages <k>] | [-linkurl url] | [-linkmd5 md5] | [-dumplinks] | [-stats]");
            return;
        }

        int i = 0;
        NutchFileSystem nfs = NutchFileSystem.parseArgs(argv, i);
        File root = new File(argv[i++]);
        DistributedWebDBReader reader = new DistributedWebDBReader(nfs, root);
        try {
            String cmd = argv[i++];

            if ("-pageurl".equals(cmd)) {
                String url = argv[i++];
                System.out.println(reader.getPage(url.trim()));
            } else if ("-pagemd5".equals(cmd)) {
                MD5Hash md5 = new MD5Hash(argv[i++]);
                Page pages[] = reader.getPages(md5);
                System.out.println("Found " + pages.length + " pages.");
                for (int j = 0; j < pages.length; j++) {
                    System.out.println("Page " + j + ": " + pages[j]);
                }
            } else if ("-dumppageurl".equals(cmd)) {
                int j = 1;
                for (Enumeration e = reader.pages(); e.hasMoreElements(); j++) {
                    Page page = (Page) e.nextElement();
                    System.out.println("Page " + j + ": " + page);
                    System.out.println();
                }
            } else if ("-dumppagemd5".equals(cmd)) {
                int j = 1;
                for (Enumeration e = reader.pagesByMD5(); e.hasMoreElements(); j++) {
                    Page page = (Page) e.nextElement();
                    System.out.println("Page " + j + ": " + page);
                    System.out.println();
                }
            } else if ("-toppages".equals(cmd)) {
                int topSize = Integer.parseInt(argv[i++]);

                // Create a sorted list
                SortedSet topSet = new TreeSet(new Comparator() {
                    public int compare(Object o1, Object o2) {
                        Page p1 = (Page) o1;
                        Page p2 = (Page) o2;
                        if (p1.getScore() < p2.getScore()) {
                            return -1;
                        } else if (p1.getScore() == p2.getScore()) {
                            // If two scores are equal, we will
                            // use regular Page comparison (which
                            // uses URL as the primary key).  We
                            // don't want to uniquify by score!
                            return p1.compareTo(p2);
                        } else {
                            return 1;
                        }
                    }
                }
                    );

                // Find the top "topSize" elts
                Page lowestPage = null;
                for (Enumeration e = reader.pages(); e.hasMoreElements(); ) {
                    Page curPage = (Page) e.nextElement();
                    if (topSet.size() < topSize) {
                        topSet.add(curPage);
                        lowestPage = (Page) topSet.first();
                    } else if (lowestPage.getScore() < curPage.getScore()) {
                        topSet.remove(lowestPage);
                        topSet.add(curPage);
                        lowestPage = (Page) topSet.first();
                    }
                }
            
                // Print them out
                int j = 0;
                for (Iterator it = topSet.iterator(); it.hasNext(); j++) {
                    System.out.println("Page " + j + ": " + (Page) it.next());
                    System.out.println();
                }
            } else if ("-linkurl".equals(cmd)) {
                String url = argv[i++];
                Link links[] = reader.getLinks(new UTF8(url.trim()));
                System.out.println("Found " + links.length + " links.");
                for (int j = 0; j < links.length; j++) {
                    System.out.println("Link " + j + ": " + links[j]);
                }
            } else if ("-linkmd5".equals(cmd)) {
                MD5Hash fromID = new MD5Hash(argv[i++]);
                Link links[] = reader.getLinks(fromID);
                System.out.println("Found " + links.length + " links.");
                for (int j = 0; j < links.length; j++) {
                    System.out.println("Link " + j + ": " + links[j]);
                }
            } else if ("-dumplinks".equals(cmd)) {
                int j = 1;
                for (Enumeration e = reader.links(); e.hasMoreElements(); j++) {
                    Link link = (Link) e.nextElement();
                    System.out.println("Link " + j + ": " + link);
                    System.out.println();
                }
            } else if ("-stats".equals(cmd)) {
                System.out.println("Stats for " + reader);
                System.out.println("-------------------------------");
                System.out.println("Number of pages: " + reader.numPages());
                System.out.println("Number of links: " + reader.numLinks());
                System.out.println("Number of machines (sections): " + reader.numMachines());
            } else {
                System.out.println("Sorry, no command with name " + cmd);
            }
        } finally {
            reader.close();
        }
    }
}
