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

package org.apache.nutch.searcher;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.Logger;
import javax.servlet.ServletContext;

import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.indexer.*;

/** 
 * One stop shopping for search-related functionality.
 * @version $Id: NutchBean.java,v 1.19 2005/02/07 19:10:08 cutting Exp $
 */   
public class NutchBean
  implements Searcher, HitDetailer, HitSummarizer, HitContent {

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.searcher.NutchBean");

  static {
    LogFormatter.setShowThreadIDs(true);
  }

  private String[] segmentNames;

  private Searcher searcher;
  private HitDetailer detailer;
  private HitSummarizer summarizer;
  private HitContent content;

  private float RAW_HITS_FACTOR =
    NutchConf.get().getFloat("searcher.hostgrouping.rawhits.factor", 2.0f);

  /** BooleanQuery won't permit more than 32 required/prohibited clauses.  We
   * don't want to use too many of those. */ 
  private static final int MAX_PROHIBITED_TERMS = 20;

  /** Cache in servlet context. */
  public static NutchBean get(ServletContext app) throws IOException {
    NutchBean bean = (NutchBean)app.getAttribute("nutchBean");
    if (bean == null) {
      LOG.info("creating new bean");
      bean = new NutchBean();
      app.setAttribute("nutchBean", bean);
    }
    return bean;
  }

  /** Construct reading from connected directory. */
  public NutchBean() throws IOException {
    this(new File(NutchConf.get().get("searcher.dir", ".")));
  }

  /** Construct in a named directory. */
  public NutchBean(File dir) throws IOException {
    File servers = new File(dir, "search-servers.txt");
    if (servers.exists()) {
      LOG.info("searching servers in " + servers.getCanonicalPath());
      init(new DistributedSearch.Client(servers));
    } else {
      init(new File(dir, "index"), new File(dir, "segments"));
    }
  }

  private void init(File indexDir, File segmentsDir) throws IOException {
    IndexSearcher indexSearcher;
    if (indexDir.exists()) {
      LOG.info("opening merged index in " + indexDir.getCanonicalPath());
      indexSearcher = new IndexSearcher(indexDir.getCanonicalPath());
    } else {
      LOG.info("opening segment indexes in " + segmentsDir.getCanonicalPath());
      
      Vector vDirs=new Vector();
      File [] directories = segmentsDir.listFiles();
      for(int i = 0; i < segmentsDir.listFiles().length; i++) {
        File indexdone = new File(directories[i], IndexSegment.DONE_NAME);
        if(indexdone.exists() && indexdone.isFile()) {
          vDirs.add(directories[i]);
        }
      }
      
      directories = new File[ vDirs.size() ];
      for(int i = 0; vDirs.size()>0; i++) {
        directories[i]=(File)vDirs.remove(0);
      }
      
      indexSearcher = new IndexSearcher(directories);
    }

    FetchedSegments segments = new FetchedSegments(new LocalFileSystem(), segmentsDir.toString());
    
    this.segmentNames = segments.getSegmentNames();
    
    this.searcher = indexSearcher;
    this.detailer = indexSearcher;
    this.summarizer = segments;
    this.content = segments;
  }

  private void init(DistributedSearch.Client client) throws IOException {
    this.segmentNames = client.getSegmentNames();
    this.searcher = client;
    this.detailer = client;
    this.summarizer = client;
    this.content = client;
  }


  public String[] getSegmentNames() {
    return segmentNames;
  }

  public Hits search(Query query, int numHits) throws IOException {
    return searcher.search(query, numHits);
  }
  
  private class SiteHits extends ArrayList {
    private boolean maxSizeExceeded;
  }

  /**
   * Search for pages matching a query, eliminating excessive hits from sites.
   * Hits for a site in excess of <code>maxHitsPerSite</code> are removed from
   * the results.  The remaining hits for such sites have {@link
   * Hit#moreFromSiteExcluded()} set.
   * <p>
   * If maxHitsPerSite is zero then all hits are returned.
   * 
   * @param query query
   * @param numHits number of requested hits
   * @param maxHitsPerSite the maximum hits returned per site, or zero
   * @return Hits the matching hits
   * @throws IOException
   */
  public Hits search(Query query, int numHits, int maxHitsPerSite)
       throws IOException {
    if (maxHitsPerSite <= 0)                      // disable site checking
      return searcher.search(query, numHits);

    int numHitsRaw = (int)(numHits * RAW_HITS_FACTOR);
    LOG.info("searching for "+numHitsRaw+" raw hits");
    Hits hits = searcher.search(query, numHitsRaw);
    long total = hits.getTotal();
    Map siteToHits = new HashMap();
    List resultList = new ArrayList();
    Set seen = new HashSet();
    List excludedSites = new ArrayList();
    boolean totalIsExact = true;
    for (int rawHitNum = 0; rawHitNum < hits.getTotal(); rawHitNum++) {
      // get the next raw hit
      if (rawHitNum >= hits.getLength()) {
        // optimize query by prohibiting more matches on some excluded sites
        Query optQuery = (Query)query.clone();
        for (int i = 0; i < excludedSites.size(); i++) {
          if (i == MAX_PROHIBITED_TERMS)
            break;
          optQuery.addProhibitedTerm(((String)excludedSites.get(i)), "site");
        }
        numHitsRaw = (int)(numHitsRaw * RAW_HITS_FACTOR);
        LOG.info("re-searching for "+numHitsRaw+" raw hits, query: "+optQuery);
        hits = searcher.search(optQuery, numHitsRaw);
        LOG.info("found "+hits.getTotal()+" raw hits");
        rawHitNum = 0;
        continue;
      }

      Hit hit = hits.getHit(rawHitNum);
      if (seen.contains(hit))
        continue;
      seen.add(hit);
      
      // get site hits for its site
      String site = hit.getSite();
      SiteHits siteHits = (SiteHits)siteToHits.get(site);
      if (siteHits == null)
        siteToHits.put(site, siteHits = new SiteHits());

      // does this hit exceed maxHitsPerSite?
      if (siteHits.size() == maxHitsPerSite) {    // yes -- ignore the hit
        if (!siteHits.maxSizeExceeded) {

          // mark prior hits with moreFromSiteExcluded
          for (int i = 0; i < siteHits.size(); i++) {
            ((Hit)siteHits.get(i)).setMoreFromSiteExcluded(true);
          }
          siteHits.maxSizeExceeded = true;

          excludedSites.add(site);                // exclude site
        }
        totalIsExact = false;
      } else {                                    // no -- collect the hit
        resultList.add(hit);
        siteHits.add(hit);

        // are we done?
        // we need to find one more than asked for, so that we can tell if
        // there are more hits to be shown
        if (resultList.size() > numHits)
          break;
      }
    }

    Hits results =
      new Hits(total,
               (Hit[])resultList.toArray(new Hit[resultList.size()]));
    results.setTotalIsExact(totalIsExact);
    return results;
  }
    

  public String getExplanation(Query query, Hit hit) throws IOException {
    return searcher.getExplanation(query, hit);
  }

  public HitDetails getDetails(Hit hit) throws IOException {
    return detailer.getDetails(hit);
  }

  public HitDetails[] getDetails(Hit[] hits) throws IOException {
    return detailer.getDetails(hits);
  }

  public String getSummary(HitDetails hit, Query query) throws IOException {
    return summarizer.getSummary(hit, query);
  }

  public String[] getSummary(HitDetails[] hits, Query query)
    throws IOException {
    return summarizer.getSummary(hits, query);
  }

  public byte[] getContent(HitDetails hit) throws IOException {
    return content.getContent(hit);
  }

  public ParseData getParseData(HitDetails hit) throws IOException {
    return content.getParseData(hit);
  }

  public ParseText getParseText(HitDetails hit) throws IOException {
    return content.getParseText(hit);
  }

  public String[] getAnchors(HitDetails hit) throws IOException {
    return content.getAnchors(hit);
  }

  public long getFetchDate(HitDetails hit) throws IOException {
    return content.getFetchDate(hit);
  }

  /** For debugging. */
  public static void main(String[] args) throws Exception {
    String usage = "NutchBean query";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    NutchBean bean = new NutchBean();
    Query query = Query.parse(args[0]);

    Hits hits = bean.search(query, 10);
    System.out.println("Total hits: " + hits.getTotal());
    int length = (int)Math.min(hits.getTotal(), 10);
    Hit[] show = hits.getHits(0, length);
    HitDetails[] details = bean.getDetails(show);
    String[] summaries = bean.getSummary(details, query);

    for (int i = 0; i < hits.getLength(); i++) {
      System.out.println(" "+i+" "+ details[i]);// + "\n" + summaries[i]);
    }
  }



}
