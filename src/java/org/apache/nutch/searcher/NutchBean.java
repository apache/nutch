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
import java.net.InetSocketAddress;
import java.util.*;

import javax.servlet.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.parse.*;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.util.NutchConfiguration;

/**
 * One stop shopping for search-related functionality.
 * @version $Id: NutchBean.java,v 1.19 2005/02/07 19:10:08 cutting Exp $
 */
public class NutchBean
implements SearchBean, SegmentBean, HitInlinks, Closeable {

  public static final Log LOG = LogFactory.getLog(NutchBean.class);
  public static final String KEY = "nutchBean";

//  static {
//    LogFormatter.setShowThreadIDs(true);
//  }

  private String[] segmentNames;

  private SearchBean searchBean;
  private SegmentBean segmentBean;
  private final HitInlinks linkDb;


  /** BooleanQuery won't permit more than 32 required/prohibited clauses.  We
   * don't want to use too many of those. */
  private static final int MAX_PROHIBITED_TERMS = 20;

  private final Configuration conf;

  private final FileSystem fs;

  /** Returns the cached instance in the servlet context.
   * @see NutchBeanConstructor*/
  public static NutchBean get(ServletContext app, Configuration conf) throws IOException {
    final NutchBean bean = (NutchBean)app.getAttribute(KEY);
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
   * Construct in a named directory.
   *
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
    final Path luceneConfig = new Path(dir, "search-servers.txt");
    final Path solrConfig = new Path(dir, "solr-servers.txt");
    final Path segmentConfig = new Path(dir, "segment-servers.txt");

    if (fs.exists(luceneConfig) || fs.exists(solrConfig)) {
      searchBean = new DistributedSearchBean(conf, luceneConfig, solrConfig);
    } else {
      final Path indexDir = new Path(dir, "index");
      final Path indexesDir = new Path(dir, "indexes");
      searchBean = new LuceneSearchBean(conf, indexDir, indexesDir);
    }

    if (fs.exists(segmentConfig)) {
      segmentBean = new DistributedSegmentBean(conf, segmentConfig);
    } else if (fs.exists(luceneConfig)) {
      segmentBean = new DistributedSegmentBean(conf, luceneConfig);
    } else {
      segmentBean = new FetchedSegments(conf, new Path(dir, "segments"));
    }

    linkDb = new LinkDbInlinks(fs, new Path(dir, "linkdb"), conf);
  }

  public static List<InetSocketAddress> readAddresses(Path path,
      Configuration conf) throws IOException {
    final List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
    for (final String line : readConfig(path, conf)) {
      final StringTokenizer tokens = new StringTokenizer(line);
      if (tokens.hasMoreTokens()) {
        final String host = tokens.nextToken();
        if (tokens.hasMoreTokens()) {
          final String port = tokens.nextToken();
          addrs.add(new InetSocketAddress(host, Integer.parseInt(port)));
        }
      }
    }
    return addrs;
  }

  public static List<String> readConfig(Path path, Configuration conf)
  throws IOException {
    final FileSystem fs = FileSystem.get(conf);
    final BufferedReader reader =
      new BufferedReader(new InputStreamReader(fs.open(path)));
    try {
      final ArrayList<String> addrs = new ArrayList<String>();
      String line;
      while ((line = reader.readLine()) != null) {
        addrs.add(line);
      }
      return addrs;
    } finally {
      reader.close();
    }
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

    return searchBean.search(query, numHits, dedupField, sortField, reverse);
  }

  @SuppressWarnings("serial")
  private class DupHits extends ArrayList<Hit> {
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

    final float rawHitsFactor = this.conf.getFloat("searcher.hostgrouping.rawhits.factor", 2.0f);
    int numHitsRaw = (int)(numHits * rawHitsFactor);
    if (LOG.isInfoEnabled()) {
      LOG.info("searching for "+numHitsRaw+" raw hits");
    }
    Hits hits = searchBean.search(query, numHitsRaw,
                                dedupField, sortField, reverse);
    final long total = hits.getTotal();
    final Map<String, DupHits> dupToHits = new HashMap<String, DupHits>();
    final List<Hit> resultList = new ArrayList<Hit>();
    final Set<Hit> seen = new HashSet<Hit>();
    final List<String> excludedValues = new ArrayList<String>();
    boolean totalIsExact = true;
    for (int rawHitNum = 0; rawHitNum < hits.getTotal(); rawHitNum++) {
      // get the next raw hit
      if (rawHitNum >= hits.getLength()) {
        // optimize query by prohibiting more matches on some excluded values
        final Query optQuery = (Query)query.clone();
        for (int i = 0; i < excludedValues.size(); i++) {
          if (i == MAX_PROHIBITED_TERMS)
            break;
          optQuery.addProhibitedTerm(excludedValues.get(i),
                                     dedupField);
        }
        numHitsRaw = (int)(numHitsRaw * rawHitsFactor);
        if (LOG.isInfoEnabled()) {
          LOG.info("re-searching for "+numHitsRaw+" raw hits, query: "+optQuery);
        }
        hits = searchBean.search(optQuery, numHitsRaw,
                               dedupField, sortField, reverse);
        if (LOG.isInfoEnabled()) {
          LOG.info("found "+hits.getTotal()+" raw hits");
        }
        rawHitNum = -1;
        continue;
      }

      final Hit hit = hits.getHit(rawHitNum);
      if (seen.contains(hit))
        continue;
      seen.add(hit);

      // get dup hits for its value
      final String value = hit.getDedupValue();
      DupHits dupHits = dupToHits.get(value);
      if (dupHits == null)
        dupToHits.put(value, dupHits = new DupHits());

      // does this hit exceed maxHitsPerDup?
      if (dupHits.size() == maxHitsPerDup) {      // yes -- ignore the hit
        if (!dupHits.maxSizeExceeded) {

          // mark prior hits with moreFromDupExcluded
          for (int i = 0; i < dupHits.size(); i++) {
            dupHits.get(i).setMoreFromDupExcluded(true);
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

    final Hits results =
      new Hits(total,
               resultList.toArray(new Hit[resultList.size()]));
    results.setTotalIsExact(totalIsExact);
    return results;
  }


  public String getExplanation(Query query, Hit hit) throws IOException {
    return searchBean.getExplanation(query, hit);
  }

  public HitDetails getDetails(Hit hit) throws IOException {
    return searchBean.getDetails(hit);
  }

  public HitDetails[] getDetails(Hit[] hits) throws IOException {
    return searchBean.getDetails(hits);
  }

  public Summary getSummary(HitDetails hit, Query query) throws IOException {
    return segmentBean.getSummary(hit, query);
  }

  public Summary[] getSummary(HitDetails[] hits, Query query)
    throws IOException {
    return segmentBean.getSummary(hits, query);
  }

  public byte[] getContent(HitDetails hit) throws IOException {
    return segmentBean.getContent(hit);
  }

  public ParseData getParseData(HitDetails hit) throws IOException {
    return segmentBean.getParseData(hit);
  }

  public ParseText getParseText(HitDetails hit) throws IOException {
    return segmentBean.getParseText(hit);
  }

  public String[] getAnchors(HitDetails hit) throws IOException {
    return linkDb.getAnchors(hit);
  }

  public Inlinks getInlinks(HitDetails hit) throws IOException {
    return linkDb.getInlinks(hit);
  }

  public long getFetchDate(HitDetails hit) throws IOException {
    return segmentBean.getFetchDate(hit);
  }

  public void close() throws IOException {
    if (searchBean != null) { searchBean.close(); }
    if (segmentBean != null) { segmentBean.close(); }
    if (linkDb != null) { linkDb.close(); }
    if (fs != null) { fs.close(); }
  }

  public boolean ping() {
    return true;
  }

  /** For debugging. */
  public static void main(String[] args) throws Exception {
    final String usage = "NutchBean query";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    final Configuration conf = NutchConfiguration.create();
    final NutchBean bean = new NutchBean(conf);
    final Query query = Query.parse(args[0], conf);
    final Hits hits = bean.search(query, 10);
    System.out.println("Total hits: " + hits.getTotal());
    final int length = (int)Math.min(hits.getTotal(), 10);
    final Hit[] show = hits.getHits(0, length);
    final HitDetails[] details = bean.getDetails(show);
    final Summary[] summaries = bean.getSummary(details, query);

    for (int i = 0; i < hits.getLength(); i++) {
      System.out.println(" " + i + " " + details[i] + "\n" + summaries[i]);
    }
  }

  public long getProtocolVersion(String className, long clientVersion)
  throws IOException {
    if(RPCSearchBean.class.getName().equals(className) &&
       searchBean instanceof RPCSearchBean) {

      final RPCSearchBean rpcBean = (RPCSearchBean)searchBean;
      return rpcBean.getProtocolVersion(className, clientVersion);
    } else if (SegmentBean.class.getName().equals(className) &&
               segmentBean instanceof RPCSegmentBean) {

      final RPCSegmentBean rpcBean = (RPCSegmentBean)segmentBean;
      return rpcBean.getProtocolVersion(className, clientVersion);
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
      final ServletContext app = sce.getServletContext();
      final Configuration conf = NutchConfiguration.get(app);

      LOG.info("creating new bean");
      NutchBean bean = null;
      try {
        bean = new NutchBean(conf);
        app.setAttribute(KEY, bean);
      }
      catch (final IOException ex) {
        LOG.error(StringUtils.stringifyException(ex));
      }
    }
  }

}
