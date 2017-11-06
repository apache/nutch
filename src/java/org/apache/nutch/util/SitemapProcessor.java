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

package org.apache.nutch.util;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.hostdb.HostDatum;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.sitemaps.AbstractSiteMap;
import crawlercommons.sitemaps.SiteMap;
import crawlercommons.sitemaps.SiteMapIndex;
import crawlercommons.sitemaps.SiteMapParser;
import crawlercommons.sitemaps.SiteMapURL;

/**
 * <p>Performs Sitemap processing by fetching sitemap links, parsing the content and merging
 * the urls from Sitemap (with the metadata) with the existing crawldb.</p>
 *
 * <p>There are two use cases supported in Nutch's Sitemap processing:</p>
 * <ol>
 *  <li>Sitemaps are considered as "remote seed lists". Crawl administrators can prepare a
 *     list of sitemap links and get only those sitemap pages. This suits well for targeted
 *     crawl of specific hosts.</li>
 *  <li>For open web crawl, it is not possible to track each host and get the sitemap links
 *     manually. Nutch would automatically get the sitemaps for all the hosts seen in the
 *     crawls and inject the urls from sitemap to the crawldb.</li>
 * </ol>
 *
 * <p>For more details see:
 *      https://wiki.apache.org/nutch/SitemapFeature </p>
 */
public class SitemapProcessor extends Configured implements Tool {
  public static final Logger LOG = LoggerFactory.getLogger(SitemapProcessor.class);
  public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public static final String CURRENT_NAME = "current";
  public static final String LOCK_NAME = ".locked";
  public static final String SITEMAP_STRICT_PARSING = "sitemap.strict.parsing";
  public static final String SITEMAP_URL_FILTERING = "sitemap.url.filter";
  public static final String SITEMAP_URL_NORMALIZING = "sitemap.url.normalize";
  public static final String SITEMAP_ALWAYS_TRY_SITEMAPXML_ON_ROOT = "sitemap.url.default.sitemap.xml";
  public static final String SITEMAP_OVERWRITE_EXISTING = "sitemap.url.overwrite.existing";

  private static class SitemapMapper extends Mapper<Text, Writable, Text, CrawlDatum> {
    private ProtocolFactory protocolFactory = null;
    private boolean strict = true;
    private boolean filter = true;
    private boolean normalize = true;
    private boolean tryDefaultSitemapXml = true;
    private URLFilters filters = null;
    private URLNormalizers normalizers = null;
    private CrawlDatum datum = new CrawlDatum();
    private SiteMapParser parser = null;

    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      this.protocolFactory = new ProtocolFactory(conf);
      this.filter = conf.getBoolean(SITEMAP_URL_FILTERING, true);
      this.normalize = conf.getBoolean(SITEMAP_URL_NORMALIZING, true);
      this.strict = conf.getBoolean(SITEMAP_STRICT_PARSING, true);
      this.tryDefaultSitemapXml = conf.getBoolean(SITEMAP_ALWAYS_TRY_SITEMAPXML_ON_ROOT, true);
      this.parser = new SiteMapParser(strict);

      if (filter) {
        filters = new URLFilters(conf);
      }
      if (normalize) {
        normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_DEFAULT);
      }
    }

    public void map(Text key, Writable value, Context context) throws IOException, InterruptedException {
      String url;

      try {
        if (value instanceof CrawlDatum) {
          // If its an entry from CrawlDb, emit it. It will be merged in the reducer
          context.write(key, (CrawlDatum) value);
        }
        else if (value instanceof HostDatum) {
          // For entry from hostdb, get sitemap url(s) from robots.txt, fetch the sitemap,
          // extract urls and emit those

          // try different combinations of schemes one by one till we get rejection in all cases
          String host = key.toString();
          if((url = filterNormalize("http://" + host + "/")) == null &&
              (url = filterNormalize("https://" + host + "/")) == null &&
              (url = filterNormalize("ftp://" + host + "/")) == null &&
              (url = filterNormalize("file:/" + host + "/")) == null) {
            context.getCounter("Sitemap", "filtered_records").increment(1);
            return;
          }
          // We may wish to use the robots.txt content as the third parameter for .getRobotRules
          BaseRobotRules rules = protocolFactory.getProtocol(url).getRobotRules(new Text(url), datum, null);
          List<String> sitemaps = rules.getSitemaps();

          if (tryDefaultSitemapXml && sitemaps.size() == 0) {
            sitemaps.add(url + "sitemap.xml");
          }
          for(String sitemap: sitemaps) {
            context.getCounter("Sitemap", "sitemaps_from_hostdb").increment(1);
            generateSitemapUrlDatum(protocolFactory.getProtocol(sitemap), sitemap, context);
          }
        }
        else if (value instanceof Text) {
          // For entry from sitemap urls file, fetch the sitemap, extract urls and emit those
          if((url = filterNormalize(key.toString())) == null) {
            context.getCounter("Sitemap", "filtered_records").increment(1);
            return;
          }

          context.getCounter("Sitemap", "sitemap_seeds").increment(1);
          generateSitemapUrlDatum(protocolFactory.getProtocol(url), url, context);
        }
      } catch (Exception e) {
        LOG.warn("Exception for record {} : {}", key.toString(), StringUtils.stringifyException(e));
      }
    }

    /* Filters and or normalizes the input URL */
    private String filterNormalize(String url) {
      try {
        if (normalizers != null)
          url = normalizers.normalize(url, URLNormalizers.SCOPE_DEFAULT);

        if (filters != null)
          url = filters.filter(url);
      } catch (Exception e) {
        return null;
      }
      return url;
    }

    private void generateSitemapUrlDatum(Protocol protocol, String url, Context context) throws Exception {
      ProtocolOutput output = protocol.getProtocolOutput(new Text(url), datum);
      ProtocolStatus status = output.getStatus();
      Content content = output.getContent();

      // Following redirects http > https
      if (!output.getStatus().isSuccess() && output.getStatus().isRedirect()) {
        String[] stuff = output.getStatus().getArgs();
        url = stuff[0];

        if (normalizers != null) {
          url = normalizers.normalize(url, URLNormalizers.SCOPE_DEFAULT);
        }

        // try again
        output = protocol.getProtocolOutput(new Text(url), datum);
        status = output.getStatus();
        content = output.getContent();
      }

      if(status.getCode() != ProtocolStatus.SUCCESS) {
        // If there were any problems fetching the sitemap, log the error and let it go. Not sure how often
        // sitemaps are redirected. In future we might have to handle redirects.
        context.getCounter("Sitemap", "failed_fetches").increment(1);
        LOG.error("Error while fetching the sitemap. Status code: {} for {}", status.getCode(), url);
        return;
      }

      AbstractSiteMap asm = parser.parseSiteMap(content.getContentType(), content.getContent(), new URL(url));

      if(asm instanceof SiteMap) {
        SiteMap sm = (SiteMap) asm;
        Collection<SiteMapURL> sitemapUrls = sm.getSiteMapUrls();
        for(SiteMapURL sitemapUrl: sitemapUrls) {
          // If 'strict' is ON, only allow valid urls. Else allow all urls
          if(!strict || sitemapUrl.isValid()) {
            String key = filterNormalize(sitemapUrl.getUrl().toString());

            if (key != null) {
              CrawlDatum sitemapUrlDatum = new CrawlDatum();
              sitemapUrlDatum.setStatus(CrawlDatum.STATUS_INJECTED);
              sitemapUrlDatum.setScore((float) sitemapUrl.getPriority());

              if(sitemapUrl.getChangeFrequency() != null) {
                int fetchInterval = -1;
                switch(sitemapUrl.getChangeFrequency()) {
                  case ALWAYS:  fetchInterval = 1;        break;
                  case HOURLY:  fetchInterval = 3600;     break; // 60*60
                  case DAILY:   fetchInterval = 86400;    break; // 60*60*24
                  case WEEKLY:  fetchInterval = 604800;   break; // 60*60*24*7
                  case MONTHLY: fetchInterval = 2592000;  break; // 60*60*24*30
                  case YEARLY:  fetchInterval = 31536000; break; // 60*60*24*365
                  case NEVER:   fetchInterval = Integer.MAX_VALUE; break; // Loose "NEVER" contract
                }
                sitemapUrlDatum.setFetchInterval(fetchInterval);
              }

              if(sitemapUrl.getLastModified() != null) {
                sitemapUrlDatum.setModifiedTime(sitemapUrl.getLastModified().getTime());
              }

              context.write(new Text(key), sitemapUrlDatum);
            }
          }
        }
      }
      else if (asm instanceof SiteMapIndex) {
        SiteMapIndex index = (SiteMapIndex) asm;
        Collection<AbstractSiteMap> sitemapUrls = index.getSitemaps();

        for(AbstractSiteMap sitemap: sitemapUrls) {
          if(sitemap.isIndex()) {
            generateSitemapUrlDatum(protocol, sitemap.getUrl().toString(), context);
          }
        }
      }
    }
  }

  private static class SitemapReducer extends Reducer<Text, CrawlDatum, Text, CrawlDatum> {
    CrawlDatum sitemapDatum  = null;
    CrawlDatum originalDatum = null;

    private boolean overwriteExisting = false; // DO NOT ENABLE!!

    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      this.overwriteExisting = conf.getBoolean(SITEMAP_OVERWRITE_EXISTING, false);
    }

    public void reduce(Text key, Iterable<CrawlDatum> values, Context context)
        throws IOException, InterruptedException {
      sitemapDatum  = null;
      originalDatum = null;

      for (CrawlDatum curr: values) {
        if(curr.getStatus() == CrawlDatum.STATUS_INJECTED) {
          sitemapDatum = new CrawlDatum();
          sitemapDatum.set(curr);
        }
        else {
          originalDatum = new CrawlDatum();
          originalDatum.set(curr);
        }
      }

      if(originalDatum != null) {
        // The url was already present in crawldb. If we got the same url from sitemap too, save
        // the information from sitemap to the original datum. Emit the original crawl datum
        if(sitemapDatum != null && overwriteExisting) {
          originalDatum.setScore(sitemapDatum.getScore());
          originalDatum.setFetchInterval(sitemapDatum.getFetchInterval());
          originalDatum.setModifiedTime(sitemapDatum.getModifiedTime());
        }

        context.getCounter("Sitemap", "existing_sitemap_entries").increment(1);
        context.write(key, originalDatum);
      }
      else if(sitemapDatum != null) {
        // For the newly discovered links via sitemap, set the status as unfetched and emit
        context.getCounter("Sitemap", "new_sitemap_entries").increment(1);
        sitemapDatum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
        context.write(key, sitemapDatum);
      }
    }
  }

  public void sitemap(Path crawldb, Path hostdb, Path sitemapUrlDir, boolean strict, boolean filter,
                      boolean normalize, int threads) throws Exception {
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("SitemapProcessor: Starting at {}", sdf.format(start));
    }

    FileSystem fs = FileSystem.get(getConf());
    Path old = new Path(crawldb, "old");
    Path current = new Path(crawldb, "current");
    Path tempCrawlDb = new Path(crawldb, "crawldb-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    // lock an existing crawldb to prevent multiple simultaneous updates
    Path lock = new Path(crawldb, LOCK_NAME);
    if (!fs.exists(current))
      fs.mkdirs(current);

    LockUtil.createLockFile(fs, lock, false);

    Configuration conf = getConf();
    conf.setBoolean(SITEMAP_STRICT_PARSING, strict);
    conf.setBoolean(SITEMAP_URL_FILTERING, filter);
    conf.setBoolean(SITEMAP_URL_NORMALIZING, normalize);
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

    Job job = Job.getInstance(conf, "SitemapProcessor_" + crawldb.toString());
    job.setJarByClass(SitemapProcessor.class);

    // add crawlDb, sitemap url directory and hostDb to input paths
    MultipleInputs.addInputPath(job, current, SequenceFileInputFormat.class);

    if (sitemapUrlDir != null)
      MultipleInputs.addInputPath(job, sitemapUrlDir, KeyValueTextInputFormat.class);

    if (hostdb != null)
      MultipleInputs.addInputPath(job, new Path(hostdb, CURRENT_NAME), SequenceFileInputFormat.class);

    FileOutputFormat.setOutputPath(job, tempCrawlDb);

    job.setOutputFormatClass(MapFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    job.setMapperClass(MultithreadedMapper.class);
    MultithreadedMapper.setMapperClass(job, SitemapMapper.class);
    MultithreadedMapper.setNumberOfThreads(job, threads);
    job.setReducerClass(SitemapReducer.class);

    try {
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "SitemapProcessor_" + crawldb.toString()
            + " job did not succeed, job status: " + job.getStatus().getState()
            + ", reason: " + job.getStatus().getFailureInfo();
        LOG.error(message);
        cleanupAfterFailure(tempCrawlDb, lock, fs);
        // throw exception so that calling routine can exit with error
        throw new RuntimeException(message);
      }

      boolean preserveBackup = conf.getBoolean("db.preserve.backup", true);
      if (!preserveBackup && fs.exists(old))
        fs.delete(old, true);
      else
        FSUtils.replace(fs, old, current, true);

      FSUtils.replace(fs, current, tempCrawlDb, true);
      LockUtil.removeLockFile(fs, lock);

      if (LOG.isInfoEnabled()) {
        long filteredRecords = job.getCounters().findCounter("Sitemap", "filtered_records").getValue();
        long fromHostDb = job.getCounters().findCounter("Sitemap", "sitemaps_from_hostdb").getValue();
        long fromSeeds = job.getCounters().findCounter("Sitemap", "sitemap_seeds").getValue();
        long failedFetches = job.getCounters().findCounter("Sitemap", "failed_fetches").getValue();
        long newSitemapEntries = job.getCounters().findCounter("Sitemap", "new_sitemap_entries").getValue();

        LOG.info("SitemapProcessor: Total records rejected by filters: {}", filteredRecords);
        LOG.info("SitemapProcessor: Total sitemaps from HostDb: {}", fromHostDb);
        LOG.info("SitemapProcessor: Total sitemaps from seed urls: {}", fromSeeds);
        LOG.info("SitemapProcessor: Total failed sitemap fetches: {}", failedFetches);
        LOG.info("SitemapProcessor: Total new sitemap entries added: {}", newSitemapEntries);

        long end = System.currentTimeMillis();
        LOG.info("SitemapProcessor: Finished at {}, elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("SitemapProcessor_" + crawldb.toString(), e);
      cleanupAfterFailure(tempCrawlDb, lock, fs);
      throw e;
    }
  }

  public void cleanupAfterFailure(Path tempCrawlDb, Path lock, FileSystem fs)
      throws IOException {
    try {
      if (fs.exists(tempCrawlDb)) {
        fs.delete(tempCrawlDb, true);
      }
      LockUtil.removeLockFile(fs, lock);
    } catch (IOException e) {
      throw e;
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new SitemapProcessor(), args);
    System.exit(res);
  }

  public static void usage() {
    System.err.println("Usage:\n SitemapProcessor <crawldb> [-hostdb <hostdb>] [-sitemapUrls <url_dir>] " +
        "[-threads <threads>] [-force] [-noStrict] [-noFilter] [-noNormalize]\n");

    System.err.println("\t<crawldb>\t\tpath to crawldb where the sitemap urls would be injected");
    System.err.println("\t-hostdb <hostdb>\tpath of a hostdb. Sitemap(s) from these hosts would be downloaded");
    System.err.println("\t-sitemapUrls <url_dir>\tpath to sitemap urls directory");
    System.err.println("\t-threads <threads>\tNumber of threads created per mapper to fetch sitemap urls");
    System.err.println("\t-force\t\t\tforce update even if CrawlDb appears to be locked (CAUTION advised)");
    System.err.println("\t-noStrict\t\tBy default Sitemap parser rejects invalid urls. '-noStrict' disables that.");
    System.err.println("\t-noFilter\t\tturn off URLFilters on urls (optional)");
    System.err.println("\t-noNormalize\t\tturn off URLNormalizer on urls (optional)");
  }

  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      usage();
      return -1;
    }

    Path crawlDb = new Path(args[0]);
    Path hostDb = null;
    Path urlDir = null;
    boolean strict = true;
    boolean filter = true;
    boolean normalize = true;
    int threads = getConf().getInt("mapred.map.multithreadedrunner.threads", 8);

    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-hostdb")) {
        hostDb = new Path(args[++i]);
        LOG.info("SitemapProcessor: hostdb: {}", hostDb);
      }
      else if (args[i].equals("-sitemapUrls")) {
        urlDir = new Path(args[++i]);
        LOG.info("SitemapProcessor: sitemap urls dir: {}", urlDir);
      }
      else if (args[i].equals("-threads")) {
        threads = Integer.valueOf(args[++i]);
        LOG.info("SitemapProcessor: threads: {}", threads);
      }
      else if (args[i].equals("-noStrict")) {
        LOG.info("SitemapProcessor: 'strict' parsing disabled");
        strict = false;
      }
      else if (args[i].equals("-noFilter")) {
        LOG.info("SitemapProcessor: filtering disabled");
        filter = false;
      }
      else if (args[i].equals("-noNormalize")) {
        LOG.info("SitemapProcessor: normalizing disabled");
        normalize = false;
      }
      else {
        LOG.info("SitemapProcessor: Found invalid argument \"{}\"\n", args[i]);
        usage();
        return -1;
      }
    }

    try {
      sitemap(crawlDb, hostDb, urlDir, strict, filter, normalize, threads);
      return 0;
    } catch (Exception e) {
      LOG.error("SitemapProcessor: {}", StringUtils.stringifyException(e));
      return -1;
    }
  }
}
