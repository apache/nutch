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

import org.apache.nutch.db.*;
import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;
import org.apache.nutch.linkdb.*;
import org.apache.nutch.pagedb.*;

/***********************************************
 * DBTester runs a test suite against 
 * org.apache.nutch.db.IWebDBWriter and org.apache.nutch.db.IWebDBReader.
 *
 * It tests things by repeatedly:
 * 1.  Adding new items and editing existing items in the WebDB,
 * 2.  Closing down the db
 * 3.  Making sure it's still coherent, via WebDBReader
 * 4.  Goto 1 a bunch of times.
 * 5.  Test the full IWebDBReader API.
 *
 * @author Mike Cafarella
 ***********************************************/
public class DBTester {
    static int MAX_OUTLINKS = 20;

    NutchFileSystem nfs;
    long seed;
    Random rand;
    File webdb;
    int maxPages;
    TreeSet seenLinks = new TreeSet();
    TreeMap md5Hashes = new TreeMap();
    long pageCount = 0, linkCount = 0, totalLinksEver = 0;
    Page pages[];
    Vector outlinks[];
    Hashtable inlinks;

    /**
     */
    public DBTester(NutchFileSystem nfs, File dir, int maxPages) throws IOException {
	this(nfs, dir, new Random().nextLong(), maxPages);
    }

    /**
     * Create a tester object by passing in the location of the
     * webdb and a few parameters.
     */
    public DBTester(NutchFileSystem nfs, File dir, long seed, int maxPages) throws IOException {
        this.nfs = nfs;
        this.maxPages = maxPages;
        this.webdb = new File(dir, "webdb_test");
        if (webdb.exists()) {
            throw new IOException("File " + webdb + " already exists");
        }
        webdb.mkdirs();
        this.seed = seed;

        WebDBWriter.createWebDB(nfs, webdb);

        this.rand = new Random(seed);
        System.out.println("-----------------------------------------------");
        System.out.println("DBTester created at " + new Date(System.currentTimeMillis()));
        System.out.println("WebDB: " + webdb);
        System.out.println("Seed: " + seed);
        System.out.println("-----------------------------------------------");


        //
        // Create structures to hold our model
        // webgraph.  As we build our own mini-web, 
        // we also make changes to the WebDB.  Then we
        // read the WebDB back and check if it matches
        // our model.
        pages = new Page[maxPages];
        outlinks = new Vector[maxPages];
        for (int i = 0; i < outlinks.length; i++) {
            outlinks[i] = new Vector();
        }
        inlinks = new Hashtable();
    }

    /**
     * We run a series of tests against the WebDB.  We do
     * this by first creating a model of our mini web-graph.
     * We insert this into the webdb, close it, and then
     * read back to make sure it matches our model.
     *
     * Then, we make a series of edits to the graph, making
     * then simultaneously to the IWebDBWriter.  Again we
     * close it down, then read it back to see if it matches.
     *
     * We do this repeatedly.
     */
    public void runTest() throws IOException {
        // Round 1.
        // First thing, we create a large number of 
        // brand-new pages.  We just add them to the
        // webdb.  Their interlink-stats are very roughly 
        // reflective of the real world.
        System.out.println("CREATING WEB MODEL, CHECKING CONSISTENCY");
        createGraph();
        // Check to see if it's correct.
        checkConsistency();

        // Round 2 (repeated)
        //
        // Next, we create a number of edits.  We 
        // select an existing page with some probability
        // p, and choose a brand-new page with (1 - p).
        // Perform some kind of edit to our model's page,
        // then make the appropriate call to IWebDBWriter.
        //
        // After all that, check the db consistency
        //
        int maxTests = 10;
        for (int i = 1; i <= maxTests; i++) {
            System.out.println("EDIT-CONSISTENCY TEST  (" + i + " of " + maxTests + ")");
            makeEdits();
            checkConsistency();
        }

        // At this point we're sure that the db is consistent
        // with our own model.  But is it self-consistent?
        // The final step is do an API-coverage test.
        System.out.println("API TEST");
        apiTest();

        //
        // Finally, we make a bunch of random page deletes from
        // the db, and test to make sure all Pages and Links are
        // removed properly.  This tests the "pageGone" scenario.
        //
        System.out.println("DB PAGE-DELETE TEST");
        IWebDBReader db = new WebDBReader(nfs, webdb);
        Vector toRemove = new Vector();
        try {
            for (Enumeration e = db.pages(); e.hasMoreElements(); ) {
                Page p = (Page) e.nextElement();
                
                if (Math.abs(rand.nextInt()) % 100 == 0) {
                    toRemove.add(p);
                }
            }
        } finally {
            db.close();
        }

        //
        // Remove the randomly-chosen elements
        //
        IWebDBWriter dbwriter = new WebDBWriter(nfs, webdb);
        try {
            for (Enumeration e = toRemove.elements(); e.hasMoreElements(); ) {
                Page p = (Page) e.nextElement();
                dbwriter.deletePage(p.getURL().toString());
            }
        } finally {
            dbwriter.close();
        }

        // Test that the Pages and any inlinks are gone
        db = new WebDBReader(nfs, webdb);
        try {
            for (Enumeration e = toRemove.elements(); e.hasMoreElements(); ) {
                Page p = (Page) e.nextElement();

                Page result = db.getPage(p.getURL().toString());
                if (result != null) {
                    // error
                    throw new IOException("Found a Page that should have been deleted: " + result);
                }

                Link results[] = db.getLinks(p.getURL());
                if (results.length != 0) {
                    // error
                    throw new IOException("Should find no inlinks for deleted URL " + p.getURL() + ", but found " + results.length + " of them.");
                }
            }
        } finally {
            db.close();
        }            

        System.out.println("*** TEST COMPLETE ***");
    }

    /**
     * Do away with the database.  Only do this if you
     * no longer need the evidence!
     */
    public void cleanup() throws IOException {
        FileUtil.fullyDelete(nfs, webdb);
    }

    /**
     * We create the 1st iteration of the web graph.  That
     * means no edits or modifications.  Just adds.
     */
    private void createGraph() throws IOException {
        IWebDBWriter writer = new WebDBWriter(nfs, webdb);
        try {
            for (int i = 0; i < maxPages; i++) {
                // Make some pages
                pages[i] = createRandomPage();
                writer.addPage(pages[i]);
                pageCount++;
            }

            // Make some links that interconnect them
            for (int i = 0; i < maxPages; i++) {
                pages[i].setNumOutlinks(makeOutlinkSet(writer, i));
            }
        } finally {
            writer.close();
        }
    }

    /**
     * We make a set of adds, deletes, and mods to the
     * internal web graph.  All of these are also applied
     * to the WebDB.
     */
    private void makeEdits() throws IOException {
        IWebDBWriter writer = new WebDBWriter(nfs, webdb);
        try {
            int actions[] = new int[pages.length];
            
            for (int i = 0; i < maxPages; i++) {
                Page curPage = pages[i];

                // We will either delete, edit, or leave it alone
                int action = Math.abs(rand.nextInt() % 2);
                actions[i] = action;
                if (action == 0) {
                    // Get rid of the page
                    Integer hashCount = (Integer) md5Hashes.get(curPage.getMD5());
                    if (hashCount.intValue() == 1) {
                        md5Hashes.remove(curPage.getMD5());
                    } else {
                        md5Hashes.put(curPage.getMD5(), new Integer(hashCount.intValue() - 1));
                    }
                    pages[i] = null;
                    writer.deletePage(curPage.getURL().toString());
                    linkCount -= outlinks[i].size();
                    
                    // Delete all the outlinks from our webgraph
                    // structures.
                    // 
                    // First, iterate through the list of all the outlinks
                    // we're about to get rid of.
                    for (Enumeration e = outlinks[i].elements(); e.hasMoreElements(); ) {
                        Link curOutlink = (Link) e.nextElement();

                        // Remove each outlink from the "seenLinks" table.
                        seenLinks.remove(curOutlink);

                        // Remove each outlink from the "inlink" tables.
                        // We need to find the target URL's inlinkList,
                        // and remove the handle to the curOutlink.
                        //
                        // First, get the target's inlinkList.
                        int removeIndex = -1, pos = 0;
                        Vector inlinkList = (Vector) inlinks.get(curOutlink.getURL().toString());
                        // Find the position where the curOutlink appears.
                        for (Enumeration e2 = inlinkList.elements(); e2.hasMoreElements(); pos++) {
                            Link curInlink = (Link) e2.nextElement();
                            if (curInlink.getFromID().equals(curOutlink.getFromID())) {
                                removeIndex = pos;
                                break;
                            }
                        }

                        // Remove the curOutlink from the target's inlink list
                        if (removeIndex >= 0) {
                            inlinkList.removeElementAt(removeIndex);
                        }
                    }

                    // Just clear all the links out.
                    outlinks[i].clear();

                    // Create a new one to replace it!
                    pages[i] = createRandomPage();

                    // Will add new links after this loop, 
                    // once all pages are created.
                } else if (action == 1) {
                    // Modify the page's MD5.
                    Integer hashCount = (Integer) md5Hashes.get(curPage.getMD5());
                    if (hashCount.intValue() == 1) {
                        md5Hashes.remove(curPage.getMD5());
                    } else {
                        md5Hashes.put(curPage.getMD5(), new Integer(hashCount.intValue() - 1));
                    }

                    // We need a unique md5 hash, because
                    // otherwise we need to maintain models
                    // of page contents.  That is too much
                    // for now, though might eventually
                    // be a good idea.
                    MD5Hash md5Hash = null;
                    do {
                        md5Hash = MD5Hash.digest(createRandomString(Math.abs(rand.nextInt() % 2048)));
                        hashCount = (Integer) md5Hashes.get(md5Hash);
                    } while (hashCount != null);

                    md5Hashes.put(md5Hash, new Integer(1));
                    pages[i].setMD5(md5Hash);

                    // We're going to generate new Outlinks.
                    // (However, the Page's URL stays the same,
                    // so all inlinks to this URL remain untouched.)
                    linkCount -= outlinks[i].size();

                    //
                    // Delete all of the outlinks from our webgraph
                    // structures.
                    //
                    // First, iterate through the list of all the outlinks
                    // we're about to get rid of.
                    for (Enumeration e = outlinks[i].elements(); e.hasMoreElements(); ) {
                        Link curOutlink = (Link) e.nextElement();

                        // Remove each outlink from the "seenLinks" table
                        seenLinks.remove(curOutlink);

                        // Remove each outlink from the "inlink" tables.
                        // We need to find the target URL's inlinkList,
                        // and remove the handle to the curOutlink.
                        //
                        // First, get the target's inlinkList
                        int removeIndex = -1, pos = 0;                        
                        Vector inlinkList = (Vector) inlinks.get(curOutlink.getURL().toString());
                        // Find the position where the curOutlink appears
                        for (Enumeration e2 = inlinkList.elements(); e2.hasMoreElements(); pos++) {
                            Link curLink = (Link) e2.nextElement();
                            if (curLink.getFromID().equals(curOutlink.getFromID())) {
                                removeIndex = pos;
                                break;
                            }
                        }
                        
                        // Remove the curOutlink from the target's inlink list
                        if (removeIndex >= 0) {
                            inlinkList.removeElementAt(removeIndex);
                        }
                    }

                    // Clear all the links out.
                    // Set the Page's number of outlinks to zero.
                    outlinks[i].clear();
                    pages[i].setNumOutlinks(0);

                    // Will add new links after this loop...
                } 
                // Otherwise, leave things alone!
            }

            // Now that we've built all the pages, add in the
            // outlinks
            for (int i = 0; i < maxPages; i++) {
                if ((actions[i] == 0) || (actions[i] == 1)) {
                    // Make the necessary outlinks for this new page!
                    pages[i].setNumOutlinks(makeOutlinkSet(writer, i));
                    writer.addPage(pages[i]);
                }
            }
        } finally {
            writer.close();
        }
    }

    /**
     * The checkConsistency() function will load in the
     * db from disk, and match it against the in-memory
     * representation.
     */
    private void checkConsistency() throws IOException {
        IWebDBReader reader = new WebDBReader(nfs, webdb);
        try {
            // Make sure counts match.
            if (pageCount != reader.numPages()) {
                throw new IOException("DB claims " + reader.numPages() + " pages, but should be " + pageCount);
            }

            if (seenLinks.size() != reader.numLinks()) {
                throw new IOException("DB claims " + reader.numLinks() + " links, but should be " + seenLinks.size() + ".  Total links since last checkConsistency: " + totalLinksEver);
            }

            // Go through every page....
            for (int i = 0; i < pageCount; i++) {
                // First, check coverage of the page set.
                Page dbPage = reader.getPage(pages[i].getURL().toString());
                if (dbPage == null) {
                    throw new IOException("DB could not find page " + pages[i].getURL());
                }
                if (! dbPage.getURL().equals(pages[i].getURL())) {
                    throw new IOException("DB's page " + dbPage.getURL() + " should be " + pages[i].getURL());
                }
                if (! dbPage.getMD5().equals(pages[i].getMD5())) {
                    throw new IOException("Page " + pages[i].getURL() + " in the DB has an MD5 of " + dbPage.getMD5() + ", but should be " + pages[i].getMD5());
                }

                // Next, the outlinks from that page.  Go through
                // every one of the links we think it should have,
                // and make sure it is there.
                Link dbOutlinks[] = reader.getLinks(pages[i].getMD5());
                for (Enumeration e = outlinks[i].elements(); e.hasMoreElements(); ) {
                    Link curOutlink = (Link) e.nextElement();
                    boolean foundLink = false;
                    for (int j = 0; j < dbOutlinks.length; j++) {
                        if (dbOutlinks[j].compareTo(curOutlink) == 0) {
                            foundLink = true;
                            break;
                        }
                    }
                    if (! foundLink) {
                        throw new IOException("DB did not return Link " + curOutlink + " when asked for all links from " + pages[i].getMD5());
                    }
                }

                // We also want to test whether there are some 
                // links in the DB which should not be there.
                // (Yes, this is caught by the above counting
                // test, but we want to find out *which* urls
                // are the "extra" ones.)
                int numTooMany = 0;
                boolean excessLinks = false;
                for (int j = 0; j < dbOutlinks.length; j++) {
                    boolean foundLink = false;
                    for (Enumeration e = outlinks[i].elements(); e.hasMoreElements(); ) {
                        Link curOutlink = (Link) e.nextElement();
                        if (dbOutlinks[j].compareTo(curOutlink) == 0) {
                            foundLink = true;
                            break;
                        }
                    }

                    if (! foundLink) {
                        System.out.println("Found excess link in WebDB: " + dbOutlinks[j]);
                        excessLinks = true;
                        numTooMany++;
                    }
                }
                if (excessLinks) {
                    throw new IOException("DB has " + numTooMany + " too many outlinks.");
                }



                // Finally, the links *to* that page.
                Vector inlinkList = (Vector) inlinks.get(pages[i].getURL().toString());
                if (inlinkList != null) {
                    Link dbInlinks[] = reader.getLinks(pages[i].getURL());
                    for (Enumeration e = inlinkList.elements(); e.hasMoreElements(); ) {
                        Link curInlink = (Link) e.nextElement();
                        boolean foundLink = false;
                        for (int j = 0; j < dbInlinks.length; j++) {
                            if (dbInlinks[j].compareTo(curInlink) == 0) {
                                foundLink = true;
                                break;
                            }
                        }
                        if (! foundLink) {
                            throw new IOException("DB did not return Link " + curInlink + " when asked for all links to " + pages[i].getURL());
                        }
                    }
                }
            }
        } finally {
            reader.close();
        }
        totalLinksEver = 0;
    }

    /**
     * apiTest() will run through all the methods of
     * IWebDBReader and make sure they give correct
     * answers.  We might use the internal model as a 
     * source of items that are in the webdb, but we aren't 
     * trying to perform a full consistency check - that's 
     * done in checkConsistency().
     */
    private void apiTest() throws IOException {
        long urlEnumCount = 0, md5EnumCount = 0, linkEnumCount = 0;
        IWebDBReader reader = new WebDBReader(nfs, webdb);
        try {
            //
            // PAGE OPERATIONS
            //

            // 1.  Test pages() and numPages()
            System.out.println("Testing IWebDBReader.pages()...");
            Page prevPage = null;
            for (Enumeration e = reader.pages(); e.hasMoreElements(); ) {
                if (prevPage == null) {
                    prevPage = (Page) e.nextElement();
                } else {
                    Page curPage = (Page) e.nextElement();
                    if (! (prevPage.getURL().compareTo(curPage.getURL()) < 0)) {
                        throw new IOException("While enumerating by URL, page " + prevPage + " comes before " + curPage);
                    }
                    prevPage = curPage;
                }
                urlEnumCount++;
            }
            if (urlEnumCount != reader.numPages()) {
                throw new IOException("IWebDBReader call to pages() results in " + urlEnumCount + ", but IWebDBReader reports " + reader.numPages() + " items.");
            }

            // 2.  Test pagesByMD5().
            System.out.println("Testing IWebDBReader.pagesByMD5()...");
            prevPage = null;
            for (Enumeration e = reader.pagesByMD5(); e.hasMoreElements(); ) {
                if (prevPage == null) {
                    prevPage = (Page) e.nextElement();
                } else {
                    Page curPage = (Page) e.nextElement();
                    if (! (prevPage.compareTo(curPage) < 0)) {
                        throw new IOException("While enumerating by MD5, page " + prevPage + " comes before " + curPage);
                    }
                    prevPage = curPage;
                }
                md5EnumCount++;
            }
            if (md5EnumCount != reader.numPages()) {
                throw new IOException("IWebDBReader call to pagesByMD5() results in " + md5EnumCount + ", but IWebDBReader reports " + reader.numPages() + " items.");
            }

            // 3.  Test getPage(String) method.
            System.out.println("Testing IWebDBReader.getPage()...");
            for (int i = 0; i < pages.length; i++) {
                Page curPage = pages[i];
                Page resultPage = reader.getPage(curPage.getURL().toString());

                if (resultPage == null || (resultPage.compareTo(curPage) != 0)) {
                    throw new IOException("Call to IWebDBReader.getPage(" + curPage.getURL() + ") should have returned " + curPage + ", but returned " + resultPage + " instead.");
                }
            }

            // 4.  Test getPages(MD5Hash)
            System.out.println("Testing IWebDBReader.getPages()...");
            for (Iterator it = md5Hashes.keySet().iterator(); it.hasNext(); ) {
                MD5Hash curHash = (MD5Hash) it.next();
                Page pageSet[] = reader.getPages(curHash);
                int numItems = ((Integer) md5Hashes.get(curHash)).intValue();
                if (pageSet.length != numItems) {
                    throw new IOException("There should be " + numItems + " item(s) with MD5Hash " + curHash + " in the db, but IWebDBReader.getPages() reports " + pageSet.length);
                }
            }

            // 5.  Test pageExists(MD5Hash)
            System.out.println("Testing IWebDBReader.pageExists()...");
            for (int i = 0; i < pages.length; i++) {
                Page curPage = pages[i];
                if (! reader.pageExists(curPage.getMD5())) {
                    throw new IOException("IWebDBReader.pageExists() reports that a page with MD5 " + curPage.getMD5() + " is not found.  It should be!");
                }
            }


            //
            // LINK OPERATIONS
            //
            
            // 1.  Test links() and numLinks(), and generate a list
            // of items to test later on.
            System.out.println("Testing IWebDBReader.links()...");
            Link prevLink = null;
            for (Enumeration e = reader.links(); e.hasMoreElements(); ) {
                if (prevLink == null) {
                    prevLink = (Link) e.nextElement();
                } else {
                    Link curLink = (Link) e.nextElement();
                    if (! (prevLink.compareTo(curLink) < 0)) {
                        throw new IOException("While enumerating by Link, link " + prevLink + " comes before " + curLink);
                    }
                    prevLink = curLink;
                }
                linkEnumCount++;
            }
            if (linkEnumCount != reader.numLinks()) {
                throw new IOException("IWebDBReader call to links() results in " + linkEnumCount + ", but IWebDBReader reports " + reader.numLinks() + " items.");
            }

            // 2.  Test getLinks(UTF8)
            System.out.println("Testing IWebDBReader.getLinks(UTF8)...");
            for (int i = 0; i < pages.length; i++) {
                Page curPage = pages[i];
                Vector inlinkList = (Vector) inlinks.get(curPage.getURL().toString());
                Link dbInlinks[] = reader.getLinks(curPage.getURL());

                if (inlinkList == null || dbInlinks == null) {
                    if ((inlinkList == null || inlinkList.size() == 0) &&
                        (dbInlinks.length != 0)) {
                        throw new IOException("Call to IWebDBReader.getLinks(" + curPage.getURL()+ ") should return 0 links, but returns " + dbInlinks.length + " instead.");
                    }
                } else {
                    if (dbInlinks.length != inlinkList.size()) {
                        throw new IOException("Call to IWebDBReader.getLinks(" + curPage.getURL() + ") should return " + inlinkList.size() + " inlinks, but returns " + dbInlinks.length + " instead.");
                    }
                }
            }

            // 3.  Test getLinks(MD5Hash)
            System.out.println("Testing IWebDBReader.getLinks(MD5Hash)...");
            for (int i = 0; i < pages.length; i++) {
                Page curPage = pages[i];
                Link dbOutlinks[] = reader.getLinks(curPage.getMD5());
                if (dbOutlinks.length != outlinks[i].size()) {
                    throw new IOException("Call to IWebDBReader.getLinks(" + curPage.getMD5() + ") should return " + outlinks[i].size() + " outlinks, but returns " + dbOutlinks.length + " instead.");
                }
                if (dbOutlinks.length != curPage.getNumOutlinks()) {
                    throw new IOException("Call to IWebDBReader.getLinks(" + curPage.getMD5() + ") should (according to Page.getNumOutlinks() return " + curPage.getNumOutlinks() + ", but returns " + dbOutlinks.length + " instead.");
                }
            }
        } finally {
            reader.close();
        }
    }

    /**
     * Return a string of numChars length.  This is good for
     * anchors and URLs.
     */
    private String createRandomString(int numChars) {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < numChars; i++) {
            buf.append((char) ('A' + Math.abs(rand.nextInt() % 26)));
        }
        return buf.toString();
    }

    /**
     * Internal utility method that manufactures a brand-new
     * novel page.
     */
    private Page createRandomPage() throws IOException {
        String curURL = "http://www.somePage." + createRandomString(20) + ".com/index.html";
        MD5Hash md5Hash = null;
        Integer hashCount = null;

        // Keep generating random contents until we have a unique
        // one.  otherwise we need to maintain models of page contents.
        // As mentioned, that's too much for now, but maybe someday.
        do {
            md5Hash = MD5Hash.digest(createRandomString(Math.abs(rand.nextInt() % 2048) + 1));
            hashCount = (Integer) md5Hashes.get(md5Hash);
        } while (hashCount != null);

        md5Hashes.put(md5Hash, new Integer(1));
        return new Page(curURL, md5Hash);
    }

    /**
     * The createClonePage() is used to test duplicate MD5s
     * in our webdb.  This is why we worry about tracking MD5s
     * in the md5Hashes table.  REMIND - It is not used yet
     */
    private Page createClonePage(Page cloneSrc) throws IOException {
        String curURL = "http://www.somePage." + createRandomString(20) + ".com/index.html";
        MD5Hash md5Hash = cloneSrc.getMD5();
        Integer hashCount = (Integer) md5Hashes.get(md5Hash);
        md5Hashes.put(md5Hash, (hashCount == null) ? new Integer(1) : new Integer(hashCount.intValue() + 1));
        return new Page(curURL, md5Hash);
    }

    /**
     * Internal method that makes a Link between two
     * pages.
     */
    private Link createLink(Page src, Page dst) throws IOException {
        UTF8 targetURL = dst.getURL();
        MD5Hash srcMD5 = new MD5Hash();
        srcMD5.set(src.getMD5());
        String linkText = createRandomString(Math.abs(rand.nextInt() % 16) + 1);
        return new Link(srcMD5, src.computeDomainID(), targetURL.toString(), linkText);
    }

    /**
     * We make a randomized set of outlinks from the given
     * page to any number of other pages.
     *
     * Returns number of outlinks generated.
     */
    private int makeOutlinkSet(IWebDBWriter writer, int srcIndex) throws IOException {
        // Create the links for this new page!
        int numOutlinks = Math.abs(rand.nextInt() % MAX_OUTLINKS) + 1;
        int numInserted = 0;
        for (int j = 0; j < numOutlinks; j++) {
            int targetPageIndex = Math.abs(rand.nextInt() % (maxPages));
            Page targetPage = pages[targetPageIndex];
            Link lr = createLink(pages[srcIndex], targetPage);

            // See if we've made this link before
            if (! seenLinks.contains(lr)) {
                outlinks[srcIndex].add(lr);
                Vector inlinkList = (Vector) inlinks.get(targetPage.getURL().toString());
                if (inlinkList == null) {
                    inlinkList = new Vector();
                    inlinks.put(targetPage.getURL().toString(), inlinkList);
                }
                inlinkList.add(lr);
                writer.addLink(lr);

                linkCount++;
                totalLinksEver++;
                numInserted++;
                seenLinks.add(lr);
            }
        }
        return numInserted;
    }

    /**
     * The command-line takes a location to put temporary work
     * files, the number of pages to use in the test set, and
     * (optionally) a seed for the random num-generator.
     */
    public static void main(String argv[]) throws IOException {
        if (argv.length < 2) {
	    System.out.println("Usage: java org.apache.nutch.db.DBTester (-local | -ndfs <namenode:port>) <workingdir> <numPages> [-seed <seed>]");
            return;
        }

        // Parse args
        int i = 0;
        NutchFileSystem nfs = NutchFileSystem.parseArgs(argv, i);
        try {
            File dbDir = new File(argv[i++]);
            int numPages = Integer.parseInt(argv[i++]);

            boolean gotSeed = false;
            long seed = 0;
            for (; i < argv.length; i++) {
                if ("-seed".equals(argv[i])) {
                    gotSeed = true;
                    seed = Long.parseLong(argv[i+1]);
                    i++;
                }
            }

            DBTester tester = (gotSeed) ? new DBTester(nfs, dbDir, seed, numPages) : new DBTester(nfs, dbDir, numPages);
            try {
                tester.runTest();
            } finally {
                tester.cleanup();
            }
        } finally {
            nfs.close();
        }
    }
}
