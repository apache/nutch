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
import org.apache.nutch.pagedb.*;
import org.apache.nutch.linkdb.*;

/**********************************************
 * IWebDBReader is an interface to the consolidated
 * page/link database.  It permits all kind of read-only ops.
 *
 * This database may be implemented in several different
 * ways, which this interface hides from its user.
 *
 * @author Mike Cafarella
 **********************************************/
public interface IWebDBReader {
    /**
     * Done reading.  Release a handle on the db.
     */
    public void close() throws IOException;
    
    /**
     * Return a Page object with the given URL, if any.
     * Pages are guaranteed to be unique by URL, so there
     * can be max. 1 returned object.
     */
    public Page getPage(String url) throws IOException;

    /**
     * Return any Pages with the given MD5 checksum.  Pages
     * with different URLs often have identical checksums; this
     * can happen if the content has been copied, or a site
     * is available under several different URLs.
     */
    public Page[] getPages(MD5Hash md5) throws IOException;

    /**
     * Returns whether a Page with the given MD5 checksum is in the db.
     */
    public boolean pageExists(MD5Hash md5) throws IOException;

    /**
     * Obtain an Enumeration of all Page objects, sorted by URL
     */
    public Enumeration pages() throws IOException;

    /**
     * Obtain an Enumeration of all Page objects, sorted by MD5.
     */
    public Enumeration pagesByMD5() throws IOException;

    /**
     * Simple count of all Page objects in db.
     */
    public long numPages();

    /**
     * Return any Link objects that point to the given URL.  This
     * array can be very large if the given URL has lots of incoming
     * Links.  So large, in fact, that this method call will probably 
     * kill the process for certain URLs.
     */
    public Link[] getLinks(UTF8 url) throws IOException;

    /**
     * Return all the Link objects that originate from a document
     * with the given MD5 checksum.  These will be the outlinks for
     * the page of content described.
     */
    public Link[] getLinks(MD5Hash md5) throws IOException;

    /**
     * Obtain an Enumeration of all Link objects, sorted by target
     * URL.
     */
    public Enumeration links() throws IOException;

    /**
     * Simple count of all Link objects in db.
     */
    public long numLinks();
}
