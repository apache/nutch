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

import org.apache.nutch.pagedb.*;
import org.apache.nutch.linkdb.*;

/******************************************
 * IWebDBWriter is an interface to the consolidated
 * page/link database.  It permits certain kinds of
 * operations.
 *
 * This database may be implemented in several different
 * ways (single or muli-pass, single-machine or distributed).
 * The user of this interface has no idea which one is
 * being used.  They all commit to the IWebDBWriter contract.
 *
 * Note that changes to a webdb are finalized upon the call
 * to close().  Before the call to close() returns, any
 * readers of the database should see the db in a pristine
 * pre-write state.
 *
 * @author Mike Cafarella
 ******************************************/
public interface IWebDBWriter {
    /**
     * Flush and complete all writes to the db.
     */
    public void close() throws IOException;

    /**
     * addPage(Page page) will insert a Page object into the webdb.  
     * If the Page already exists, the existing one will be overwritten.
     * (Except for the link analysis score, which we try to attach to
     * a given URL.  If an existing Page is overwritten, we will retain
     * the link score.)
     *
     * Page objects are uniquified by their URLs.  It's fine to have
     * many Pages with different URLs but identical MD5s.  (Indeed,
     * that happens all the time with duplicated pages.)  But every
     * Page in the db must have its own URL.
     */
    public void addPage(Page page) throws IOException;

    /**
     * addPageWithScore(Page page) inserts a Page into the webdb.
     * It works just like the above function, except that link scores
     * are not preserved if the inserted object already exists.  The
     * inserted object's score will replace one that may already be there.
     *
     * This function is useful for the Link Analysis program.
     */
    public void addPageWithScore(Page page) throws IOException;

    /**
     * addPageIfNotPresent(Page) works just like addPage(), except that
     * the insertion will not take place if there is already a Page with
     * that URL in the webdb.  In that case, the call to addPage() is
     * simply ignored.
     */
    public void addPageIfNotPresent(Page page) throws IOException;    

    /**
     * addPageIfNotPresent(Page, Link) works just like the above addPage(),
     * except that a Link is also conditionally added to the webdb.
     */
    public void addPageIfNotPresent(Page page, Link link) throws IOException;    

    /**
     * deletePage(url) will remove a Page object from the db with the
     * given URL.  Fails silently if there is no Page with the given URL.
     */
    public void deletePage(String url) throws IOException;

    /**
     * addLink(Link) will add the given Link to the webdb.  If the
     * Link already exists, the existing one will be overwritten.
     *
     * Links are uniquified by both source MD5 and target URL.  
     * Two Links are considered identical only if they match both
     * fields.
     *
     * Links are only permitted in the webdb if they have a valid
     * source MD5 for a Page that is also in the webdb.  When a
     * Page is removed, the webdb will automatically remove Links
     * as appropriate.  
     *
     * (Note that since there can be multiple URLs with identical
     * content, the webdb basically needs to do reference-counting
     * for each Link's source-MD5.)
     */
    public void addLink(Link link) throws IOException;
}
