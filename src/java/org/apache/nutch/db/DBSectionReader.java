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

/**********************************************************
 * DBSectionReader reads a discrete portion of a WebDB.
 * It may implement its methods with either a local
 * MapFile.Reader object or (eventually) a remote-
 * machine network interface.  For the moment, we 
 * do only the MapFile.Reader implementation (much of
 * the code for this was moved from the earlier 
 * pre-distributed version of WebDBReadaer).
 *
 * @author Mike Cafarella
 ***********************************************/
public class DBSectionReader {
    NutchFileSystem nfs;
    File sectionFile;
    WritableComparator comparator;
    MapFile.Reader reader;

    /**
     * Right now we assume we're getting a File that is a 
     * MapFile.Reader directory.  But in the future we could
     * also check for existence of a "remote-network" file, similar
     * to the way we do now for distributed index reading.
     * Then, we would either create a MapFile.Reader or a network
     * client for one.
     */
    public DBSectionReader(NutchFileSystem nfs, File sectionFile, WritableComparator comparator) throws IOException {
        this.nfs = nfs;
        this.sectionFile = sectionFile;
        this.comparator = comparator;
        this.reader = new MapFile.Reader(nfs, sectionFile.getPath(), comparator);
    }

    /**
     * Fetch a Page with the given URL, and fill it into
     * the pre-allocated Page 'p'.
     */
    public Page getPage(UTF8 url, Page p) throws IOException {
        return (Page) reader.get(url, p);
    }

    /**
     * Get Pages from the db according to their
     * content hash.
     */
    public Vector getPages(MD5Hash md5) throws IOException {
        Vector records = new Vector(3);
        Page p = new Page();
        p.getMD5().set(md5);

        reader.seek(p);
        while (reader.next(p, NullWritable.get())) {
            if (p.getMD5().compareTo(md5) == 0) {
                records.add(p);
                p = new Page();
            } else {
                break;
            }
        }

        return records;
    }

    /**
     * Test whether a certain piece of content is in the 
     * db, but don't bother returning it.
     */
    public boolean pageExists(MD5Hash md5) throws IOException {
        Page p = new Page();
        p.getMD5().set(md5);
        reader.seek(p);
        if (reader.next(p, NullWritable.get()) && p.getMD5().compareTo(md5) == 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Iterate through all the Pages, sorted by URL
     */
    public Enumeration pages() throws IOException {
        return new TableEnumerator(new MapFile.Reader(nfs, sectionFile.getPath(), comparator));
    }

    //
    // The TableEnumerator goes through all the entries
    // in the Table (which is a MapFile).
    //
    class TableEnumerator implements Enumeration {
        MapFile.Reader reader;
        Page nextItem;

        /**
         * Start the cursor and find the first item.
         * Store it for later return.
         */
        public TableEnumerator(MapFile.Reader reader) {
            this.reader = reader;
            this.nextItem = new Page();
            try {
                if (! reader.next(new UTF8(), this.nextItem)) {
                    this.nextItem = null;
                }
            } catch (IOException ie) {
                ie.printStackTrace();
                this.nextItem = null;
            }
        }

        /**
         * If there's no item left in store, we've hit the end.
         */
        public boolean hasMoreElements() {
            return (nextItem != null);
        }

        /**
         * Set aside the item we have in store.  Then retrieve
         * another for the next time we're called.  Finally, return
         * the set-aside item.
         */
        public Object nextElement() {
            if (nextItem == null) {
                throw new NoSuchElementException("PageDB Enumeration");
            }
            Page toReturn = nextItem;
            this.nextItem = new Page();
            try {
                if (! reader.next(new UTF8(), nextItem)) {
                    this.nextItem = null;
                }
            } catch (IOException ie) {
                this.nextItem = null;
            }
            return toReturn;
        }
    }

    /**
     * Iterate through all the Pages, sorted by MD5
     */
    public Enumeration pagesByMD5() throws IOException {
        return new IndexEnumerator(new SetFile.Reader(nfs, sectionFile.getPath(), comparator));
    }

    //
    // The IndexEnumerator goes through all the entries
    // in the index (which is a SequenceFile).
    //
    class IndexEnumerator implements Enumeration {
        SetFile.Reader reader;
        Page nextItem;

        /**
         * Start the cursor and find the first item.
         * Store it for later return.
         */
        public IndexEnumerator(SetFile.Reader reader) {
            this.reader = reader;
            this.nextItem = new Page();
            try {
                if (! reader.next(nextItem)) {
                    this.nextItem = null;
                }
            } catch (IOException ie) {
                this.nextItem = null;
            }
        }

        /**
         * If there's no item left in store, we've hit the end.
         */
        public boolean hasMoreElements() {
            return (nextItem != null);
        }

        /**
         * Set aside the item we have in store.  Then retrieve
         * another for the next time we're called.  Finally, return
         * the set-aside item.
         */
        public Object nextElement() {
            if (nextItem == null) {
                throw new NoSuchElementException("PageDB Enumeration");
            }

            Page toReturn = nextItem;
            this.nextItem = new Page();
            try {
                if (! reader.next(nextItem)) {
                    this.nextItem = null;
                }
            } catch (IOException ie) {
                this.nextItem = null;
            }
            return toReturn;
        }
    }

    /**
     * Get all the hyperlinks that link TO the indicated URL.
     */     
    public Vector getLinks(UTF8 url) throws IOException {
        Vector records = new Vector(3);
        Link l = new Link();
        l.getURL().set(url);

        reader.seek(l);
        while (reader.next(l, NullWritable.get())) {
            if (url.equals(l.getURL())) {
                records.add(l);
                l = new Link();
            } else {
                break;
            }
        }
        
        return records;
    }

    /**
     * Grab all the links from the given MD5 hash.
     */
    public Vector getLinks(MD5Hash md5) throws IOException {
        Vector records = new Vector(3);
        Link l = new Link();
        l.getFromID().set(md5);

        reader.seek(l);
        while (reader.next(l, NullWritable.get())) {
            if (md5.equals(l.getFromID())) {
                records.add(l);
                l = new Link();
            } else {
                break;
            }
        }
        
        return records;
    }

    /**
     * Return all the links, by target URL
     */
    public Enumeration links() throws IOException {
        return new MapEnumerator(new MapFile.Reader(nfs, sectionFile.getPath(), comparator));
    }
    
    //
    // Here's the class for the above function
    //
    class MapEnumerator implements Enumeration {
        MapFile.Reader reader;
        Link nextItem;

        /**
         * Start the cursor and find the first item.
         * Store it for later return.
         */
        public MapEnumerator(MapFile.Reader reader) {
            this.reader = reader;
            this.nextItem = new Link();
            try {
                if (! reader.next(this.nextItem, NullWritable.get())) {
                    this.nextItem = null;
                }
            } catch (IOException ie) {
                this.nextItem = null;
            }
        }

        /**
         * If there's no item left in store, we've hit the end.
         */
        public boolean hasMoreElements() {
            return (nextItem != null);
        }

        /**
         * Set aside the item we have in store.  Then retrieve
         * another for the next time we're called.  Finally, return
         * the set-aside item.
         */
        public Object nextElement() {
            if (nextItem == null) {
                throw new NoSuchElementException("PageDB Enumeration");
            }

            Link toReturn = nextItem;
            this.nextItem = new Link();
            try {
                if (! reader.next(nextItem, NullWritable.get())) {
                    this.nextItem = null;
                }
            } catch (IOException ie) {
                this.nextItem = null;
            }
            return toReturn;
        }
    }

    /**
     */
    public void close() throws IOException {
        reader.close();
    }
}
