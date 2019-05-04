/*
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
package org.apache.nutch.scoring.webgraph;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.util.FSUtils;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.URLUtil;

/**
 * Creates three databases, one for inlinks, one for outlinks, and a node
 * database that holds the number of in and outlinks to a url and the current
 * score for the url.
 * 
 * The score is set by an analysis program such as LinkRank. The WebGraph is an
 * update-able database. Outlinks are stored by their fetch time or by the
 * current system time if no fetch time is available. Only the most recent
 * version of outlinks for a given url is stored. As more crawls are executed
 * and the WebGraph updated, newer Outlinks will replace older Outlinks. This
 * allows the WebGraph to adapt to changes in the link structure of the web.
 * 
 * The Inlink database is created from the Outlink database and is regenerated
 * when the WebGraph is updated. The Node database is created from both the
 * Inlink and Outlink databases. Because the Node database is overwritten when
 * the WebGraph is updated and because the Node database holds current scores
 * for urls it is recommended that a crawl-cycle (one or more full crawls) fully
 * complete before the WebGraph is updated and some type of analysis, such as
 * LinkRank, is run to update scores in the Node database in a stable fashion.
 */
public class WebGraph extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  public static final String LOCK_NAME = ".locked";
  public static final String INLINK_DIR = "inlinks";
  public static final String OUTLINK_DIR = "outlinks/current";
  public static final String OLD_OUTLINK_DIR = "outlinks/old";
  public static final String NODE_DIR = "nodes";

  /**
   * The OutlinkDb creates a database of all outlinks. Outlinks to internal urls
   * by domain and host can be ignored. The number of Outlinks out to a given
   * page or domain can also be limited.
   */
  public static class OutlinkDb extends Configured {

    public static final String URL_NORMALIZING = "webgraph.url.normalizers";
    public static final String URL_FILTERING = "webgraph.url.filters";

    /**
     * Returns the fetch time from the parse data or the current system time if
     * the fetch time doesn't exist.
     * 
     * @param data
     *          The parse data.
     * 
     * @return The fetch time as a long.
     */
    private static long getFetchTime(ParseData data) {

      // default to current system time
      long fetchTime = System.currentTimeMillis();
      String fetchTimeStr = data.getContentMeta().get(Nutch.FETCH_TIME_KEY);
      try {
        // get the fetch time from the parse data
        fetchTime = Long.parseLong(fetchTimeStr);
      } catch (Exception e) {
        fetchTime = System.currentTimeMillis();
      }
      return fetchTime;
    }

    /**
     * Default constructor.
     */
    public OutlinkDb() {
    }

    /**
     * Configurable constructor.
     */
    public OutlinkDb(Configuration conf) {
      setConf(conf);
    }

    /**
     * Passes through existing LinkDatum objects from an existing OutlinkDb and
     * maps out new LinkDatum objects from new crawls ParseData.
     */
    public static class OutlinkDbMapper extends
        Mapper<Text, Writable, Text, NutchWritable> {

      // using normalizers and/or filters
      private boolean normalize = false;
      private boolean filter = false;

      // url normalizers, filters and job configuration
      private URLNormalizers urlNormalizers;
      private URLFilters filters;
      private Configuration conf;

      /**
       * Normalizes and trims extra whitespace from the given url.
       * 
       * @param url
       *          The url to normalize.
       * 
       * @return The normalized url.
       */
      private String normalizeUrl(String url) {

        if (!normalize) {
          return url;
        }

        String normalized = null;
        if (urlNormalizers != null) {
          try {
            
            // normalize and trim the url
            normalized = urlNormalizers.normalize(url,
                URLNormalizers.SCOPE_DEFAULT);
            normalized = normalized.trim();
          } catch (Exception e) {
            LOG.warn("Skipping " + url + ":" + e);
            normalized = null;
          }
        }
        return normalized;
      }

      /**
       * Filters the given url.
       * 
       * @param url
       *          The url to filter.
       * 
       * @return The filtered url or null.
       */
      private String filterUrl(String url) {

        if (!filter) {
          return url;
        }

        try { 
          url = filters.filter(url);
        } catch (Exception e) {
          url = null;
        }

        return url;
      }

      /**
       * Configures the OutlinkDb job mapper. Sets up internal links and link limiting.
       */
      @Override
      public void setup(Mapper<Text, Writable, Text, NutchWritable>.Context context) {
        Configuration config = context.getConfiguration();
        conf = config;

        normalize = conf.getBoolean(URL_NORMALIZING, false);
        filter = conf.getBoolean(URL_FILTERING, false);

        if (normalize) {
          urlNormalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_DEFAULT);
        }

        if (filter) {
          filters = new URLFilters(conf);
        }
      }

      @Override
      public void map(Text key, Writable value,
          Context context)
          throws IOException, InterruptedException {

        // normalize url, stop processing if null
        String url = normalizeUrl(key.toString());
        if (url == null) {
          return;
        }

        // filter url
        if (filterUrl(url) == null) {
          return;
        }

        // Overwrite the key with the normalized URL
        key.set(url);

        if (value instanceof CrawlDatum) {
          CrawlDatum datum = (CrawlDatum) value;

          if (datum.getStatus() == CrawlDatum.STATUS_FETCH_REDIR_TEMP
              || datum.getStatus() == CrawlDatum.STATUS_FETCH_REDIR_PERM
              || datum.getStatus() == CrawlDatum.STATUS_FETCH_GONE) {

            // Tell the reducer to get rid of all instances of this key
            context.write(key, new NutchWritable(new BooleanWritable(true)));
          }
        } else if (value instanceof ParseData) {
          // get the parse data and the outlinks from the parse data, along with
          // the fetch time for those links
          ParseData data = (ParseData) value;
          long fetchTime = getFetchTime(data);
          Outlink[] outlinkAr = data.getOutlinks();
          Map<String, String> outlinkMap = new LinkedHashMap<>();

          // normalize urls and put into map
          if (outlinkAr != null && outlinkAr.length > 0) {
            for (int i = 0; i < outlinkAr.length; i++) {
              Outlink outlink = outlinkAr[i];
              String toUrl = normalizeUrl(outlink.getToUrl());

              if (filterUrl(toUrl) == null) {
                continue;
              }

              // only put into map if the url doesn't already exist in the map or
              // if it does and the anchor for that link is null, will replace if
              // url is existing
              boolean existingUrl = outlinkMap.containsKey(toUrl);
              if (toUrl != null
                  && (!existingUrl || (existingUrl && outlinkMap.get(toUrl) == null))) {
                outlinkMap.put(toUrl, outlink.getAnchor());
              }
            }
          }

          // collect the outlinks under the fetch time
          for (String outlinkUrl : outlinkMap.keySet()) {
            String anchor = outlinkMap.get(outlinkUrl);
            LinkDatum datum = new LinkDatum(outlinkUrl, anchor, fetchTime);
            context.write(key, new NutchWritable(datum));
          }
        } else if (value instanceof LinkDatum) {
          LinkDatum datum = (LinkDatum) value;
          String linkDatumUrl = normalizeUrl(datum.getUrl());

          if (filterUrl(linkDatumUrl) != null) {
            datum.setUrl(linkDatumUrl);

            // collect existing outlinks from existing OutlinkDb
            context.write(key, new NutchWritable(datum));
          }
        }
      }
    }

    public static class OutlinkDbReducer extends
        Reducer<Text, NutchWritable, Text, LinkDatum> {

      // ignoring internal domains, internal hosts
      private boolean ignoreDomain = true;
      private boolean ignoreHost = true;

      // limiting urls out to a page or to a domain
      private boolean limitPages = true;
      private boolean limitDomains = true;

      // url normalizers, filters and job configuration
      private Configuration conf;

      /**
       * Configures the OutlinkDb job reducer. Sets up internal links and link limiting.
       */
      public void setup(Reducer<Text, NutchWritable, Text, LinkDatum>.Context context) {
        Configuration config = context.getConfiguration();
        conf = config;
        ignoreHost = conf.getBoolean("link.ignore.internal.host", true);
        ignoreDomain = conf.getBoolean("link.ignore.internal.domain", true);
        limitPages = conf.getBoolean("link.ignore.limit.page", true);
        limitDomains = conf.getBoolean("link.ignore.limit.domain", true);
        
      }
   
      public void reduce(Text key, Iterable<NutchWritable> values,
          Context context)
          throws IOException, InterruptedException {

        // aggregate all outlinks, get the most recent timestamp for a fetch
        // which should be the timestamp for all of the most recent outlinks
        long mostRecent = 0L;
        List<LinkDatum> outlinkList = new ArrayList<>();
        for (NutchWritable val : values) {
          final Writable value = val.get();

          if (value instanceof LinkDatum) {
            // loop through, change out most recent timestamp if needed
            LinkDatum next = (LinkDatum) value;
            long timestamp = next.getTimestamp();
            if (mostRecent == 0L || mostRecent < timestamp) {
              mostRecent = timestamp;
            }
            outlinkList.add(WritableUtils.clone(next, conf));
            context.getCounter("WebGraph.outlinks", "added links").increment(1);
          } else if (value instanceof BooleanWritable) {
            BooleanWritable delete = (BooleanWritable) value;
            // Actually, delete is always true, otherwise we don't emit it in the
            // mapper in the first place
            if (delete.get() == true) {
              // This page is gone, do not emit it's outlinks
              context.getCounter("WebGraph.outlinks", "removed links").increment(1);
              return;
            }
          }
        }

        // get the url, domain, and host for the url
        String url = key.toString();
        String domain = URLUtil.getDomainName(url);
        String host = URLUtil.getHost(url);

        // setup checking sets for domains and pages
        Set<String> domains = new HashSet<>();
        Set<String> pages = new HashSet<>();

        // loop through the link datums
        for (LinkDatum datum : outlinkList) {

          // get the url, host, domain, and page for each outlink
          String toUrl = datum.getUrl();
          String toDomain = URLUtil.getDomainName(toUrl);
          String toHost = URLUtil.getHost(toUrl);
          String toPage = URLUtil.getPage(toUrl);
          datum.setLinkType(LinkDatum.OUTLINK);

          // outlinks must be the most recent and conform to internal url and
          // limiting rules, if it does collect it
          if (datum.getTimestamp() == mostRecent
              && (!limitPages || (limitPages && !pages.contains(toPage)))
              && (!limitDomains || (limitDomains && !domains.contains(toDomain)))
              && (!ignoreHost || (ignoreHost && !toHost.equalsIgnoreCase(host)))
              && (!ignoreDomain || (ignoreDomain && !toDomain
                  .equalsIgnoreCase(domain)))) {
            context.write(key, datum);
            pages.add(toPage);
            domains.add(toDomain);
          }
        }
      }
    }

    public void close() {
    }
  }

  /**
   * The InlinkDb creates a database of Inlinks. Inlinks are inverted from the
   * OutlinkDb LinkDatum objects and are regenerated each time the WebGraph is
   * updated.
   */
  private static class InlinkDb extends Configured{

    private static long timestamp;

    /**
     * Inverts the Outlink LinkDatum objects into new LinkDatum objects with a
     * new system timestamp, type and to and from url switched.
     */
    public static class InlinkDbMapper extends
        Mapper<Text, LinkDatum, Text, LinkDatum> {

      /**
       * Configures job mapper. Sets timestamp for all Inlink LinkDatum objects to the
       * current system time.
       */
      public void setup(Mapper<Text, LinkDatum, Text, LinkDatum>.Context context) {
        timestamp = System.currentTimeMillis();
      }

      public void map(Text key, LinkDatum datum,
          Context context)
          throws IOException, InterruptedException {

        // get the to and from url and the anchor
        String fromUrl = key.toString();
        String toUrl = datum.getUrl();
        String anchor = datum.getAnchor();

        // flip the from and to url and set the new link type
        LinkDatum inlink = new LinkDatum(fromUrl, anchor, timestamp);
        inlink.setLinkType(LinkDatum.INLINK);
        context.write(new Text(toUrl), inlink);
      }
    }
  }

  /**
   * Creates the Node database which consists of the number of in and outlinks
   * for each url and a score slot for analysis programs such as LinkRank.
   */
  private static class NodeDb extends Configured {

    /**
     * Counts the number of inlinks and outlinks for each url and sets a default
     * score of 0.0 for each url (node) in the webgraph.
     */
    public static class NodeDbReducer extends 
        Reducer<Text, LinkDatum, Text, Node> {

      /**
       * Configures job reducer.
       */
      public void setup(Reducer<Text, LinkDatum, Text, Node>.Context context) {
      }

      public void reduce(Text key, Iterable<LinkDatum> values,
          Context context) throws IOException, InterruptedException {

        Node node = new Node();
        int numInlinks = 0;
        int numOutlinks = 0;

        // loop through counting number of in and out links
        for (LinkDatum next : values) {
          if (next.getLinkType() == LinkDatum.INLINK) {
            numInlinks++;
          } else if (next.getLinkType() == LinkDatum.OUTLINK) {
            numOutlinks++;
          }
        }

        // set the in and outlinks and a default score of 0
        node.setNumInlinks(numInlinks);
        node.setNumOutlinks(numOutlinks);
        node.setInlinkScore(0.0f);
        context.write(key, node);
      }
    }
  }

  /**
   * Creates the three different WebGraph databases, Outlinks, Inlinks, and
   * Node. If a current WebGraph exists then it is updated, if it doesn't exist
   * then a new WebGraph database is created.
   * 
   * @param webGraphDb
   *          The WebGraph to create or update.
   * @param segments
   *          The array of segments used to update the WebGraph. Newer segments
   *          and fetch times will overwrite older segments.
   * @param normalize
   *          whether to use URLNormalizers on URL's in the segment
   * @param filter
   *          whether to use URLFilters on URL's in the segment
   * 
   * @throws IOException
   *           If an error occurs while processing the WebGraph.
   */
  public void createWebGraph(Path webGraphDb, Path[] segments,
      boolean normalize, boolean filter) throws IOException, 
      InterruptedException, ClassNotFoundException {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("WebGraphDb: starting at " + sdf.format(start));
      LOG.info("WebGraphDb: webgraphdb: " + webGraphDb);
      LOG.info("WebGraphDb: URL normalize: " + normalize);
      LOG.info("WebGraphDb: URL filter: " + filter);
    }

    FileSystem fs = webGraphDb.getFileSystem(getConf());

    // lock an existing webgraphdb to prevent multiple simultaneous updates
    Path lock = new Path(webGraphDb, LOCK_NAME);
    if (!fs.exists(webGraphDb)) {
      fs.mkdirs(webGraphDb);
    }

    LockUtil.createLockFile(fs, lock, false);

    // outlink and temp outlink database paths
    Path outlinkDb = new Path(webGraphDb, OUTLINK_DIR);
    Path oldOutlinkDb = new Path(webGraphDb, OLD_OUTLINK_DIR);

    if (!fs.exists(outlinkDb)) {
      fs.mkdirs(outlinkDb);
    }

    Path tempOutlinkDb = new Path(outlinkDb + "-"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
    Job outlinkJob = NutchJob.getInstance(getConf());
    Configuration outlinkJobConf = outlinkJob.getConfiguration();
    outlinkJob.setJobName("Outlinkdb: " + outlinkDb);

    boolean deleteGone = outlinkJobConf.getBoolean("link.delete.gone", false);
    boolean preserveBackup = outlinkJobConf.getBoolean("db.preserve.backup", true);

    if (deleteGone) {
      LOG.info("OutlinkDb: deleting gone links");
    }

    // get the parse data and crawl fetch data for all segments
    if (segments != null) {
      for (int i = 0; i < segments.length; i++) {
        FileSystem sfs = segments[i].getFileSystem(outlinkJobConf);
        Path parseData = new Path(segments[i], ParseData.DIR_NAME);
        if (sfs.exists(parseData)) {
          LOG.info("OutlinkDb: adding input: " + parseData);
          FileInputFormat.addInputPath(outlinkJob, parseData);
        }

        if (deleteGone) {
          Path crawlFetch = new Path(segments[i], CrawlDatum.FETCH_DIR_NAME);
          if (sfs.exists(crawlFetch)) {
            LOG.info("OutlinkDb: adding input: " + crawlFetch);
            FileInputFormat.addInputPath(outlinkJob, crawlFetch);
          }
        }
      }
    }

    // add the existing webgraph
    LOG.info("OutlinkDb: adding input: " + outlinkDb);
    FileInputFormat.addInputPath(outlinkJob, outlinkDb);

    outlinkJobConf.setBoolean(OutlinkDb.URL_NORMALIZING, normalize);
    outlinkJobConf.setBoolean(OutlinkDb.URL_FILTERING, filter);

    outlinkJob.setInputFormatClass(SequenceFileInputFormat.class);
    outlinkJob.setJarByClass(OutlinkDb.class);
    outlinkJob.setMapperClass(OutlinkDb.OutlinkDbMapper.class);
    outlinkJob.setReducerClass(OutlinkDb.OutlinkDbReducer.class);
    outlinkJob.setMapOutputKeyClass(Text.class);
    outlinkJob.setMapOutputValueClass(NutchWritable.class);
    outlinkJob.setOutputKeyClass(Text.class);
    outlinkJob.setOutputValueClass(LinkDatum.class);
    FileOutputFormat.setOutputPath(outlinkJob, tempOutlinkDb);
    outlinkJob.setOutputFormatClass(MapFileOutputFormat.class);
    outlinkJobConf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
        false);

    // run the outlinkdb job and replace any old outlinkdb with the new one
    try {
      LOG.info("OutlinkDb: running");
      boolean success = outlinkJob.waitForCompletion(true);
      if (!success) {
        String message = "OutlinkDb job did not succeed, job status:"
            + outlinkJob.getStatus().getState() + ", reason: "
            + outlinkJob.getStatus().getFailureInfo();
        LOG.error(message);
        NutchJob.cleanupAfterFailure(tempOutlinkDb, lock, fs);
        throw new RuntimeException(message);
      }
      LOG.info("OutlinkDb: installing " + outlinkDb);
      FSUtils.replace(fs, oldOutlinkDb, outlinkDb, true);
      FSUtils.replace(fs, outlinkDb, tempOutlinkDb, true);
      if (!preserveBackup && fs.exists(oldOutlinkDb))
        fs.delete(oldOutlinkDb, true);
      LOG.info("OutlinkDb: finished");
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("OutlinkDb failed:", e);
      // remove lock file and and temporary directory if an error occurs
      NutchJob.cleanupAfterFailure(tempOutlinkDb, lock, fs);
      throw e;
    }

    // inlink and temp link database paths
    Path inlinkDb = new Path(webGraphDb, INLINK_DIR);
    Path tempInlinkDb = new Path(inlinkDb + "-"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    Job inlinkJob = NutchJob.getInstance(getConf());
    Configuration inlinkJobConf = inlinkJob.getConfiguration();
    inlinkJob.setJobName("Inlinkdb " + inlinkDb);
    LOG.info("InlinkDb: adding input: " + outlinkDb);
    FileInputFormat.addInputPath(inlinkJob, outlinkDb);
    inlinkJob.setInputFormatClass(SequenceFileInputFormat.class);
    inlinkJob.setJarByClass(InlinkDb.class);
    inlinkJob.setMapperClass(InlinkDb.InlinkDbMapper.class);
    inlinkJob.setMapOutputKeyClass(Text.class);
    inlinkJob.setMapOutputValueClass(LinkDatum.class);
    inlinkJob.setOutputKeyClass(Text.class);
    inlinkJob.setOutputValueClass(LinkDatum.class);
    FileOutputFormat.setOutputPath(inlinkJob, tempInlinkDb);
    inlinkJob.setOutputFormatClass(MapFileOutputFormat.class);
    inlinkJobConf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
        false);

    try {

      // run the inlink and replace any old with new
      LOG.info("InlinkDb: running");
      boolean success = inlinkJob.waitForCompletion(true);
      if (!success) {
        String message = "InlinkDb job did not succeed, job status:"
            + inlinkJob.getStatus().getState() + ", reason: "
            + inlinkJob.getStatus().getFailureInfo();
        LOG.error(message);
        NutchJob.cleanupAfterFailure(tempInlinkDb, lock, fs);
        throw new RuntimeException(message);
      }
      LOG.info("InlinkDb: installing " + inlinkDb);
      FSUtils.replace(fs, inlinkDb, tempInlinkDb, true);
      LOG.info("InlinkDb: finished");
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("InlinkDb failed:", e);
      // remove lock file and and temporary directory if an error occurs
      NutchJob.cleanupAfterFailure(tempInlinkDb, lock, fs);
      throw e;
    }

    // node and temp node database paths
    Path nodeDb = new Path(webGraphDb, NODE_DIR);
    Path tempNodeDb = new Path(nodeDb + "-"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    Job nodeJob = NutchJob.getInstance(getConf());
    Configuration nodeJobConf = nodeJob.getConfiguration();
    nodeJob.setJobName("NodeDb " + nodeDb);
    LOG.info("NodeDb: adding input: " + outlinkDb);
    LOG.info("NodeDb: adding input: " + inlinkDb);
    FileInputFormat.addInputPath(nodeJob, outlinkDb);
    FileInputFormat.addInputPath(nodeJob, inlinkDb);
    nodeJob.setInputFormatClass(SequenceFileInputFormat.class);
    nodeJob.setJarByClass(NodeDb.class);
    nodeJob.setReducerClass(NodeDb.NodeDbReducer.class);
    nodeJob.setMapOutputKeyClass(Text.class);
    nodeJob.setMapOutputValueClass(LinkDatum.class);
    nodeJob.setOutputKeyClass(Text.class);
    nodeJob.setOutputValueClass(Node.class);
    FileOutputFormat.setOutputPath(nodeJob, tempNodeDb);
    nodeJob.setOutputFormatClass(MapFileOutputFormat.class);
    nodeJobConf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
        false);

    try {

      // run the node job and replace old nodedb with new
      LOG.info("NodeDb: running");
      boolean success = nodeJob.waitForCompletion(true);
      if (!success) {
        String message = "NodeDb job did not succeed, job status:"
            + nodeJob.getStatus().getState() + ", reason: "
            + nodeJob.getStatus().getFailureInfo();
        LOG.error(message);
        // remove lock file and and temporary directory if an error occurs
        NutchJob.cleanupAfterFailure(tempNodeDb, lock, fs);
        throw new RuntimeException(message);
      }
      LOG.info("NodeDb: installing " + nodeDb);
      FSUtils.replace(fs, nodeDb, tempNodeDb, true);
      LOG.info("NodeDb: finished");
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("NodeDb failed:", e);
      // remove lock file and and temporary directory if an error occurs
      NutchJob.cleanupAfterFailure(tempNodeDb, lock, fs);
      throw e;
    }

    // remove the lock file for the webgraph
    LockUtil.removeLockFile(fs, lock);

    long end = System.currentTimeMillis();
    LOG.info("WebGraphDb: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new WebGraph(), args);
    System.exit(res);
  }

  /**
   * Parses command link arguments and runs the WebGraph jobs.
   */
  public int run(String[] args) throws Exception {

    // boolean options
    Option helpOpt = new Option("h", "help", false, "show this help message");
    Option normOpt = new Option("n", "normalize", false,
        "whether to use URLNormalizers on the URL's in the segment");
    Option filtOpt = new Option("f", "filter", false,
        "whether to use URLFilters on the URL's in the segment");

    // argument options
    @SuppressWarnings("static-access")
    Option graphOpt = OptionBuilder
        .withArgName("webgraphdb")
        .hasArg()
        .withDescription(
            "the web graph database to create (if none exists) or use if one does")
        .create("webgraphdb");
    @SuppressWarnings("static-access")
    Option segOpt = OptionBuilder.withArgName("segment").hasArgs()
        .withDescription("the segment(s) to use").create("segment");
    @SuppressWarnings("static-access")
    Option segDirOpt = OptionBuilder.withArgName("segmentDir").hasArgs()
        .withDescription("the segment directory to use").create("segmentDir");

    // create the options
    Options options = new Options();
    options.addOption(helpOpt);
    options.addOption(normOpt);
    options.addOption(filtOpt);
    options.addOption(graphOpt);
    options.addOption(segOpt);
    options.addOption(segDirOpt);

    CommandLineParser parser = new GnuParser();
    try {
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("webgraphdb")
          || (!line.hasOption("segment") && !line.hasOption("segmentDir"))) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("WebGraph", options, true);
        return -1;
      }

      String webGraphDb = line.getOptionValue("webgraphdb");

      Path[] segPaths = null;

      // Handle segment option
      if (line.hasOption("segment")) {
        String[] segments = line.getOptionValues("segment");
        segPaths = new Path[segments.length];
        for (int i = 0; i < segments.length; i++) {
          segPaths[i] = new Path(segments[i]);
        }
      }

      // Handle segmentDir option
      if (line.hasOption("segmentDir")) {
        Path dir = new Path(line.getOptionValue("segmentDir"));
        FileSystem fs = dir.getFileSystem(getConf());
        FileStatus[] fstats = fs.listStatus(dir,
            HadoopFSUtil.getPassDirectoriesFilter(fs));
        segPaths = HadoopFSUtil.getPaths(fstats);
      }

      boolean normalize = false;

      if (line.hasOption("normalize")) {
        normalize = true;
      }

      boolean filter = false;

      if (line.hasOption("filter")) {
        filter = true;
      }

      createWebGraph(new Path(webGraphDb), segPaths, normalize, filter);
      return 0;
    } catch (Exception e) {
      LOG.error("WebGraph: " + StringUtils.stringifyException(e));
      return -2;
    }
  }

}
