/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.*;
import javax.servlet.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Closeable;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.parse.*;
import org.apache.nutch.indexer.*;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;

/** 
 * One stop shopping for search-related functionality.
 * @version $Id: NutchBean.java,v 1.19 2005/02/07 19:10:08 cutting Exp $
 */   
public class NutchBean
  implements Searcher, HitDetailer, HitSummarizer, HitContent, HitInlinks,
             DistributedSearch.Protocol, Closeable {

  public static final Log LOG = LogFactory.getLog(NutchBean.class);
  public static final String KEY = "nutchBean";

//  static {
//    LogFormatter.setShowThreadIDs(true);
//  }

  private String[] segmentNames;

  private Searcher searcher;
  private HitDetailer detailer;
  private HitSummarizer summarizer;
  private HitContent content;
  private HitInlinks linkDb;


  /** BooleanQuery won't permit more than 32 required/prohibited clauses.  We
   * don't want to use too many of those. */ 
  private static final int MAX_PROHIBITED_TERMS = 20;
  
  private Configuration conf;

  private FileSystem fs;

  /** Returns the cached instance in the servlet context. 
   * @see NutchBeanConstructor*/
  public static NutchBean get(ServletContext app, Configuration conf) throws IOException {
    NutchBean bean = (NutchBean)app.getAttribute(KEY);
    return bean;
  }


  /**
   * 
   * @param conf
   * @throws IOException
   */
  public NutchBean(Configuration conf) throws IOException {
    this(conf, null);
  }
  
  /**
   *  Construct in a named directory. 
   * @param conf
   * @param dir
   * @throws IOException
   */
  public NutchBean(Configuration conf, Path dir) throws IOException {
        this.conf = conf;
        this.fs = FileSystem.get(this.conf);
        if (dir == null) {
            dir = new Path(this.conf.get("searcher.dir", "crawl"));
        }
        Path servers = new Path(dir, "search-servers.txt");
        if (fs.exists(servers)) {
            if (LOG.isInfoEnabled()) {
              LOG.info("searching servers in " + servers);
            }
            init(new DistributedSearch.Client(servers, conf));
        } else {
            init(new Path(dir, "index"), new Path(dir, "indexes"), new Path(
                    dir, "segments"), new Path(dir, "linkdb"));
        }
    }

  private void init(Path indexDir, Path indexesDir, Path segmentsDir,
                    Path linkDb)
    throws IOException {
    IndexSearcher indexSearcher;
    if (this.fs.exists(indexDir)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("opening merged index in " + indexDir);
      }
      indexSearcher = new IndexSearcher(indexDir, this.conf);
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("opening indexes in " + indexesDir);
      }
      
      Vector vDirs=new Vector();
      Path [] directories = fs.listPaths(indexesDir, HadoopFSUtil.getPassDirectoriesFilter(fs));
      for(int i = 0; i < directories.length; i++) {
        Path indexdone = new Path(directories[i], Indexer.DONE_NAME);
        if(fs.isFile(indexdone)) {
          vDirs.add(directories[i]);
        }
      }
      
      
      directories = new Path[ vDirs.size() ];
      for(int i = 0; vDirs.size()>0; i++) {
        directories[i]=(Path)vDirs.remove(0);
      }
      
      indexSearcher = new IndexSearcher(directories, this.conf);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("opening segments in " + segmentsDir);
    }
    FetchedSegments segments = new FetchedSegments(this.fs, segmentsDir.toString(),this.conf);
    
    this.segmentNames = segments.getSegmentNames();

    this.searcher = indexSearcher;
    this.detailer = indexSearcher;
    this.summarizer = segments;
    this.content = segments;

    if (LOG.isInfoEnabled()) { LOG.info("opening linkdb in " + linkDb); }
    this.linkDb = new LinkDbInlinks(fs, linkDb, this.conf);
  }

  private void init(DistributedSearch.Client client) {
    this.segmentNames = client.getSegmentNames();
    this.searcher = client;
    this.detailer = client;
    this.summarizer = client;
    this.content = client;
    this.linkDb = client;
  }


  public String[] getSegmentNames() {
    return segmentNames;
  }

  public Hits search(Query query, int numHits) throws IOException {
    return search(query, numHits, null, null, false);
  }
  
  public Hits search(Query query, int numHits,
                     String dedupField, String sortField, boolean reverse)
    throws IOException {

    return searcher.search(query, numHits, dedupField, sortField, reverse);
  }
  
  private class DupHits extends ArrayList {
    private boolean maxSizeExceeded;
  }

  /** Search for pages matching a query, eliminating excessive hits from the
   * same site.  Hits after the first <code>maxHitsPerDup</code> from the same
   * site are removed from results.  The remaining hits have {@link
   * Hit#moreFromDupExcluded()} set.  <p> If maxHitsPerDup is zero then all
   * hits are returned.
   * 
   * @param query query
   * @param numHits number of requested hits
   * @param maxHitsPerDup the maximum hits returned with matching values, or zero
   * @return Hits the matching hits
   * @throws IOException
   */
  public Hits search(Query query, int numHits, int maxHitsPerDup)
       throws IOException {
    return search(query, numHits, maxHitsPerDup, "site", null, false);
  }

  /** Search for pages matching a query, eliminating excessive hits with
   * matching values for a named field.  Hits after the first
   * <code>maxHitsPerDup</code> are removed from results.  The remaining hits
   * have {@link Hit#moreFromDupExcluded()} set.  <p> If maxHitsPerDup is zero
   * then all hits are returned.
   * 
   * @param query query
   * @param numHits number of requested hits
   * @param maxHitsPerDup the maximum hits returned with matching values, or zero
   * @param dedupField field name to check for duplicates
   * @return Hits the matching hits
   * @throws IOException
   */
  public Hits search(Query query, int numHits,
                     int maxHitsPerDup, String dedupField)
       throws IOException {
    return search(query, numHits, maxHitsPerDup, dedupField, null, false);
  }
  /** Search for pages matching a query, eliminating excessive hits with
   * matching values for a named field.  Hits after the first
   * <code>maxHitsPerDup</code> are removed from results.  The remaining hits
   * have {@link Hit#moreFromDupExcluded()} set.  <p> If maxHitsPerDup is zero
   * then all hits are returned.
   * 
   * @param query query
   * @param numHits number of requested hits
   * @param maxHitsPerDup the maximum hits returned with matching values, or zero
   * @param dedupField field name to check for duplicates
   * @param sortField Field to sort on (or null if no sorting).
   * @param reverse True if we are to reverse sort by <code>sortField</code>.
   * @return Hits the matching hits
   * @throws IOException
   */
  public Hits search(Query query, int numHits,
                     int maxHitsPerDup, String dedupField,
                     String sortField, boolean reverse)
       throws IOException {
    if (maxHitsPerDup <= 0)                      // disable dup checking
      return search(query, numHits, dedupField, sortField, reverse);

    float rawHitsFactor = this.conf.getFloat("searcher.hostgrouping.rawhits.factor", 2.0f);
    int numHitsRaw = (int)(numHits * rawHitsFactor);
    if (LOG.isInfoEnabled()) {
      LOG.info("searching for "+numHitsRaw+" raw hits");
    }
    Hits hits = searcher.search(query, numHitsRaw,
                                dedupField, sortField, reverse);
    long total = hits.getTotal();
    Map dupToHits = new HashMap();
    List resultList = new ArrayList();
    Set seen = new HashSet();
    List excludedValues = new ArrayList();
    boolean totalIsExact = true;
    for (int rawHitNum = 0; rawHitNum < hits.getTotal(); rawHitNum++) {
      // get the next raw hit
      if (rawHitNum >= hits.getLength()) {
        // optimize query by prohibiting more matches on some excluded values
        Query optQuery = (Query)query.clone();
        for (int i = 0; i < excludedValues.size(); i++) {
          if (i == MAX_PROHIBITED_TERMS)
            break;
          optQuery.addProhibitedTerm(((String)excludedValues.get(i)),
                                     dedupField);
        }
        numHitsRaw = (int)(numHitsRaw * rawHitsFactor);
        if (LOG.isInfoEnabled()) {
          LOG.info("re-searching for "+numHitsRaw+" raw hits, query: "+optQuery);
        }
        hits = searcher.search(optQuery, numHitsRaw,
                               dedupField, sortField, reverse);
        if (LOG.isInfoEnabled()) {
          LOG.info("found "+hits.getTotal()+" raw hits");
        }
        rawHitNum = -1;
        continue;
      }

      Hit hit = hits.getHit(rawHitNum);
      if (seen.contains(hit))
        continue;
      seen.add(hit);
      
      // get dup hits for its value
      String value = hit.getDedupValue();
      DupHits dupHits = (DupHits)dupToHits.get(value);
      if (dupHits == null)
        dupToHits.put(value, dupHits = new DupHits());

      // does this hit exceed maxHitsPerDup?
      if (dupHits.size() == maxHitsPerDup) {      // yes -- ignore the hit
        if (!dupHits.maxSizeExceeded) {

          // mark prior hits with moreFromDupExcluded
          for (int i = 0; i < dupHits.size(); i++) {
            ((Hit)dupHits.get(i)).setMoreFromDupExcluded(true);
          }
          dupHits.maxSizeExceeded = true;

          excludedValues.add(value);              // exclude dup
        }
        totalIsExact = false;
      } else {                                    // no -- collect the hit
        resultList.add(hit);
        dupHits.add(hit);

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

  public Summary getSummary(HitDetails hit, Query query) throws IOException {
    return summarizer.getSummary(hit, query);
  }

  public Summary[] getSummary(HitDetails[] hits, Query query)
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
    return linkDb.getAnchors(hit);
  }

  public Inlinks getInlinks(HitDetails hit) throws IOException {
    return linkDb.getInlinks(hit);
  }

  public long getFetchDate(HitDetails hit) throws IOException {
    return content.getFetchDate(hit);
  }

  public void close() throws IOException {
    if (content != null) { content.close(); }
    if (searcher != null) { searcher.close(); }
    if (linkDb != null) { linkDb.close(); }
    if (fs != null) { fs.close(); }
  }
  
  /** For debugging. */
  public static void main(String[] args) throws Exception {
    String usage = "NutchBean query";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    Configuration conf = NutchConfiguration.create();
    NutchBean bean = new NutchBean(conf);
    Query query = Query.parse(args[0], conf);
    Hits hits = bean.search(query, 10);
    System.out.println("Total hits: " + hits.getTotal());
    int length = (int)Math.min(hits.getTotal(), 10);
    Hit[] show = hits.getHits(0, length);
    HitDetails[] details = bean.getDetails(show);
    Summary[] summaries = bean.getSummary(details, query);

    for (int i = 0; i < hits.getLength(); i++) {
      System.out.println(" "+i+" "+ details[i] + "\n" + summaries[i]);
    }
  }

  public long getProtocolVersion(String className, long arg1) throws IOException {
    if(DistributedSearch.Protocol.class.getName().equals(className)){
      return 1;
    } else {
      throw new IOException("Unknown Protocol classname:" + className);
    }
  }

  /** Responsible for constructing a NutchBean singleton instance and 
   *  caching it in the servlet context. This class should be registered in 
   *  the deployment descriptor as a listener 
   */
  public static class NutchBeanConstructor implements ServletContextListener {
    
    public void contextDestroyed(ServletContextEvent sce) { }

    public void contextInitialized(ServletContextEvent sce) {
      ServletContext app = sce.getServletContext();
      Configuration conf = NutchConfiguration.get(app);
      
      LOG.info("creating new bean");
      NutchBean bean = null;
      try {
        bean = new NutchBean(conf);
        app.setAttribute(KEY, bean);
      }
      catch (IOException ex) {
        LOG.error(StringUtils.stringifyException(ex));
      }
    }
  }

}
