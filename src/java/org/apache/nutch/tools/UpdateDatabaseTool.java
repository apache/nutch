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
import java.net.*;
import java.util.logging.*;

import org.apache.nutch.db.*;
import org.apache.nutch.net.*;
import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.linkdb.*;
import org.apache.nutch.pagedb.*;
import org.apache.nutch.fetcher.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.util.*;


/*****************************************************
 * This class takes the output of the fetcher and updates the page and link
 * DBs accordingly.  Eventually, as the database scales, this will broken into
 * several phases, each consuming and emitting batch files, but, for now, we're
 * doing it all here.
 *
 * @author Doug Cutting
 *****************************************************/
public class UpdateDatabaseTool {
    public static final float NEW_INTERNAL_LINK_FACTOR =
      NutchConf.get().getFloat("db.score.link.internal", 1.0f);
    public static final float NEW_EXTERNAL_LINK_FACTOR =
      NutchConf.get().getFloat("db.score.link.external", 1.0f);
    public static final int MAX_OUTLINKS_PER_PAGE =
      NutchConf.get().getInt("db.max.outlinks.per.page", 100);

    public static final boolean IGNORE_INTERNAL_LINKS =
      NutchConf.get().getBoolean("db.ignore.internal.links", true);


    public static final Logger LOG =
      LogFormatter.getLogger("org.apache.nutch.tools.UpdateDatabaseTool");

    private static final int MAX_RETRIES = 2;
    private static final long MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000;

    private IWebDBWriter webdb;
    private int maxCount = 0;
    private boolean additionsAllowed = true;
    private Set outlinkSet = new TreeSet(); // used in Page attr calculations

    /**
     * Take in the WebDBWriter, instantiated elsewhere.
     */
    public UpdateDatabaseTool(IWebDBWriter webdb, boolean additionsAllowed, int maxCount) {
        this.webdb = webdb;
        this.additionsAllowed = additionsAllowed;
        this.maxCount = maxCount;
    }

    /**
     * Iterate through items in the FetcherOutput.  For each one,
     * determine whether the pages need to be added to the webdb,
     * or what fields need to be changed.
     */
    public void updateForSegment(NutchFileSystem nfs, String directory)
        throws IOException {
        ArrayList deleteQueue = new ArrayList();
        String fetchDir=new File(directory, FetcherOutput.DIR_NAME).toString();
        String parseDir=new File(directory, ParseData.DIR_NAME).toString();
        ArrayFile.Reader fetch = null;
        ArrayFile.Reader parse = null;
        int count = 0;
        try {
          fetch = new ArrayFile.Reader(nfs, fetchDir);
          parse = new ArrayFile.Reader(nfs, parseDir);
          FetcherOutput fo = new FetcherOutput();
          ParseData pd = new ParseData();
          while (fetch.next(fo) != null) {
            parse.next(pd);

            if ((count % 1000) == 0) {
                LOG.info("Processing document " + count);
            }
            if ((maxCount >= 0) && (count >= maxCount)) {
              break;
            }

            FetchListEntry fle = fo.getFetchListEntry();
            Page page = fle.getPage();
            LOG.fine("Processing " + page.getURL());
            if (!fle.getFetch()) {                // didn't fetch
              pageContentsUnchanged(fo);          // treat as unchanged

            } else if (fo.getStatus() == fo.SUCCESS) { // fetch succeed
              if (fo.getMD5Hash().equals(page.getMD5())) {
                pageContentsUnchanged(fo);        // contents unchanged
              } else {
                pageContentsChanged(fo, pd);      // contents changed
              }

            } else if (fo.getStatus() == fo.RETRY &&
                       page.getRetriesSinceFetch() < MAX_RETRIES) {

              pageRetry(fo);                      // retry later

            } else {
              pageGone(fo);                       // give up: page is gone
            }
            count++;
          }
        } catch (EOFException e) {
          LOG.warning("Unexpected EOF in: " + fetchDir +
                      " at entry #" + count + ".  Ignoring.");
        } finally {
          if (fetch != null)
            fetch.close();
          if (parse != null)
            parse.close();
        }
    }

    /**
     * There's been no change: update date & retries only
     */
    private void pageContentsUnchanged(FetcherOutput fetcherOutput)
        throws IOException {
        Page oldPage = fetcherOutput.getFetchListEntry().getPage();
        Page newPage = (Page)oldPage.clone();

        LOG.fine("unchanged");

        newPage.setNextFetchTime(nextFetch(fetcherOutput)); // set next fetch
        newPage.setRetriesSinceFetch(0);              // zero retries

        webdb.addPage(newPage);                       // update record in db
    }
    
    /**
     * We've encountered new content, so update md5, etc.
     * Also insert the new outlinks into the link DB
     */
    private void pageContentsChanged(FetcherOutput fetcherOutput,
                                     ParseData parseData) throws IOException {
      Page oldPage = fetcherOutput.getFetchListEntry().getPage();
      Page newPage = (Page)oldPage.clone();

      LOG.fine("new contents");

      newPage.setNextFetchTime(nextFetch(fetcherOutput)); // set next fetch
      newPage.setMD5(fetcherOutput.getMD5Hash());   // update md5
      newPage.setRetriesSinceFetch(0);              // zero retries

      // Go through all the outlinks from this page, and add to
      // the LinkDB.
      //
      // If the replaced page is the last ref to its MD5, then
      // its outlinks must be removed.  The WebDBWriter will
      // handle that, upon page-replacement.
      //
      Outlink[] outlinks = parseData.getOutlinks();
      String sourceHost = getHost(oldPage.getURL().toString());
      long sourceDomainID = newPage.computeDomainID();
      long nextFetch = nextFetch(fetcherOutput, 0);
      outlinkSet.clear();  // Use a hashtable to uniquify the links
      int end = Math.min(outlinks.length, MAX_OUTLINKS_PER_PAGE);
      for (int i = 0; i < end; i++) {
        Outlink link = outlinks[i];
        String url = link.getToUrl();

        try {
          url = URLFilters.filter(url);
        } catch (URLFilterException e) {
          throw new IOException(e.getMessage());
        }
        if (url == null)
          continue;

        outlinkSet.add(url);        
        
        if (additionsAllowed) {
            String destHost = getHost(url);
            boolean internal = destHost == null || destHost.equals(sourceHost);

            try {
                //
                // If it is an in-site link, then we only add a Link if
                // the Page is also added.  So we pass it to addPageIfNotPresent().
                //
                // If it is not an in-site link, then we always add the link.
                // We then conditionally add the Page with addPageIfNotPresent().
                //
                Link newLink = new Link(newPage.getMD5(), sourceDomainID, url, link.getAnchor());

                float newScore = oldPage.getScore();
                float newNextScore = oldPage.getNextScore();

                if (internal) {
                  newScore *= NEW_INTERNAL_LINK_FACTOR;
                  newNextScore *= NEW_INTERNAL_LINK_FACTOR;
                } else {
                  newScore *= NEW_EXTERNAL_LINK_FACTOR;
                  newNextScore *= NEW_EXTERNAL_LINK_FACTOR;
                }

                Page linkedPage = new Page(url, newScore, newNextScore, nextFetch);

                if (internal && IGNORE_INTERNAL_LINKS) {
                  webdb.addPageIfNotPresent(linkedPage, newLink);
                } else {
                  webdb.addLink(newLink);
                  webdb.addPageIfNotPresent(linkedPage);
                }

            } catch (MalformedURLException e) {
                LOG.fine("skipping " + url + ":" + e);
            }
        }
      }

      // Calculate the number of different outlinks here.
      // We use the outlinkSet TreeSet so that we count only
      // the unique links leaving the Page.  The WebDB will
      // only store one value for each (fromID,toURL) pair
      //
      // Store the value with the Page, to speed up later
      // Link Analysis computation.
      //
      // NOTE: This value won't necessarily even match what's
      // in the linkdb!  That's OK!  It's more important that
      // this number be a "true count" of the outlinks from
      // the page in question, than the value reflect what's
      // actually in our db.  (There are a number of reasons,
      // mainly space economy, to avoid placing URLs in our db.
      // These reasons slightly pervert the "true out count".)
      // 
      newPage.setNumOutlinks(outlinkSet.size());  // Store # outlinks

      webdb.addPage(newPage);                     // update record in db
    }

    /**
     * Keep the page, but never re-fetch it.
     */
    private void pageGone(FetcherOutput fetcherOutput)
        throws IOException {
        Page oldPage = fetcherOutput.getFetchListEntry().getPage();
        Page newPage = (Page)oldPage.clone();

        LOG.fine("retry never");

        newPage.setNextFetchTime(Long.MAX_VALUE); // never refetch
        webdb.addPage(newPage);                   // update record in db
    }

    /**
     * Update with new retry count and date
     */
    private void pageRetry(FetcherOutput fetcherOutput)
        throws IOException {
        Page oldPage = fetcherOutput.getFetchListEntry().getPage();
        Page newPage = (Page)oldPage.clone();

        LOG.fine("retry later");

        newPage.setNextFetchTime(nextFetch(fetcherOutput,1)); // wait a day
        newPage.setRetriesSinceFetch
            (oldPage.getRetriesSinceFetch()+1);         // increment retries

        webdb.addPage(newPage);                       // update record in db
    }

    /**
     * Compute the next fetchtime for the Page.
     */
    private long nextFetch(FetcherOutput fo) {
        return nextFetch(fo,
                         fo.getFetchListEntry().getPage().getFetchInterval());
    }

    /**
     * Compute the next fetchtime, from this moment, with the given
     * number of days.
     */
    private long nextFetch(FetcherOutput fetcherOutput, int days) {
      return fetcherOutput.getFetchDate() + (MILLISECONDS_PER_DAY * days);
    }

    /**
     * Parse the hostname from a URL and return it.
     */
    private String getHost(String url) {
      try {
        return new URL(url).getHost().toLowerCase();
      } catch (MalformedURLException e) {
        return null;
      }
    }

    /**
     * Shut everything down.
     */
    public void close() throws IOException {
        webdb.close();
    }

    /**
     * Create the UpdateDatabaseTool, and pass in a WebDBWriter.
     */
    public static void main(String args[]) throws Exception {
      int segDirStart = -1;
      int max = -1;
      boolean additionsAllowed = true;

      String usage = "UpdateDatabaseTool (-local | -ndfs <namenode:port>) [-max N] [-noAdditions] <db> <seg_dir> [ <seg_dir> ... ]";
      if (args.length < 2) {
          System.out.println(usage);
          return;
      }

      int i = 0;
      NutchFileSystem nfs = NutchFileSystem.parseArgs(args, i);
      for (; i < args.length; i++) {     // parse command line
        if (args[i].equals("-max")) {      // found -max option
          max = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-noAdditions")) {
          additionsAllowed = false;
        } else {
            break;
        }
      }

      File root = new File(args[i++]);
      segDirStart = i;

      if (segDirStart == -1) {
        System.err.println(usage);
        System.exit(-1);
      }
      
      LOG.info("Updating " + root);

      IWebDBWriter webdb = new WebDBWriter(nfs, root);
      UpdateDatabaseTool tool = new UpdateDatabaseTool(webdb, additionsAllowed, max);
      for (i = segDirStart; i < args.length; i++) {
        String segDir = args[i];
        if (segDir != null) {
            LOG.info("Updating for " + segDir);
            tool.updateForSegment(nfs, segDir);
        }
      }

      LOG.info("Finishing update");
      tool.close();
      nfs.close();
      LOG.info("Update finished");
    }
}
