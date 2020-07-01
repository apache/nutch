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

package org.apache.nutch.crawl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolNotFound;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import crawlercommons.domains.EffectiveTldFinder;
import crawlercommons.robots.BaseRobotRules;
import crawlercommons.sitemaps.AbstractSiteMap;
import crawlercommons.sitemaps.SiteMap;
import crawlercommons.sitemaps.SiteMapIndex;
import crawlercommons.sitemaps.SiteMapParser;
import crawlercommons.sitemaps.SiteMapURL;

/**
 * Inject URLs from sitemaps (http://www.sitemaps.org/).
 *
 * Sitemap URLs are given same way as "ordinary" seeds URLs - one URL per line.
 * Each URL points to one of
 * <ul>
 * <li>XML sitemap</li>
 * <li>plain text sitemap (possibly compressed)</li>
 * <li>sitemap index (XML)</li>
 * <li>and all
 * <a href="http://www.sitemaps.org/protocol.html#otherformats">other
 * formats</a> supported by the Sitemap parser of
 * <a href="http://code.google.com/p/crawler-commons/">crawler-commons</a>.</li>
 * </ul>
 *
 * <p>
 * All sitemap URLs on the input path are fetched and the URLs contained in the
 * sitemaps are "injected" into CrawlDb. If a sitemap specifies modification
 * time, refresh rate, and/or priority for a page, these values are stored in
 * CrawlDb but adjusted so that they fit into global limits. E.g.,
 *
 * <pre>
 * &lt;changefreq&gt;yearly&lt;/changefreq&gt;
 * </pre>
 *
 * may be limited to the value of property
 * <code>db.fetch.schedule.max_interval</code> and/or
 * <code>db.fetch.interval.max</code>.
 * </p>
 *
 * The typical use case for the SitemapInjector is to feed the crawl with a list
 * of URLs maintained by the site's owner (generated, e.g., via content
 * management system).
 *
 * Fetching sitemaps is done by Nutch protocol plugins to make use of special
 * settings, e.g., HTTP proxy configurations.
 *
 * The behavior how entries in CrawlDb are overwritten by injected entries does
 * not differ from {@link Injector}.
 *
 * <h2>Limitations</h2>
 *
 * <p>
 * SitemapInjector does not support:
 * <ul>
 * <li>[done/implemented] follow redirects</li>
 * <li>no retry scheduling if fetching a sitemap fails</li>
 * <li>be polite and add delays between fetching sitemaps. Usually, there is
 * only one sitemap per host, so this does not matter that much.</li>
 * <li>[done/implemented] check for
 * &quot;<a href="http://www.sitemaps.org/protocol.html#location">cross
 * submits</a>&quot;: if a sitemap URL is explicitly given it is assumed the
 * sitemap's content is trustworthy</li>
 * </ul>
 * </p>
 */
public class SitemapInjector extends Injector {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  protected int threads = 8;
  protected boolean keepTemp;
  protected boolean runStepOneOnly;
  protected boolean runStepTwoOnly;

  /** Fetch and parse sitemaps, output extracted URLs as seeds */
  public static class SitemapInjectMapper extends InjectMapper {

    private static final String SITEMAP_MAX_URLS = "db.injector.sitemap.max_urls";
    private static final String SITEMAP_MAX_HOSTS = "db.injector.sitemap.max_hosts";
    private static final String SITEMAP_CROSS_SUBMIT_CHECK = "db.injector.sitemap.check-cross-submits";
    private static final String SITEMAP_CROSS_SUBMIT_CHECK_TYPE = "db.injector.sitemap.check-cross-submit.type";
    private static final String SITEMAP_CROSS_SUBMITS = "db.injector.sitemap.cross-submits";

    protected float minInterval;
    protected float maxInterval;

    protected int maxRecursiveSitemaps = 50001;
    /** limit for (deeply) nested sitemap indexes */
    protected int maxRecursiveSitemapDepth = 3;
    protected long maxUrlsPerSitemapIndex = 50000L * 50000;

    /**
     * Need a limit on the branching factor, sitemaps from spam hosts may refer
     * to hundreds of hosts
     */
    protected int maxHostsPerSitemapIndex = 100;

    protected int maxSitemapFetchTime = 180;
    protected int maxSitemapProcessingTime;
    protected int maxUrlLength = 512;

    protected boolean checkRobotsTxt = true;
    protected boolean checkCrossSubmits = true;

    enum CrossSubmitType {
      PUBLIC_DOMAIN, PRIVATE_DOMAIN, HOST
    }

    protected CrossSubmitType checkCrossSubmitsType = CrossSubmitType.PRIVATE_DOMAIN;

    protected int maxFailuresPerHost = 5;
    protected int maxRedirect = 3;

    private ProtocolFactory protocolFactory;
    private SiteMapParser sitemapParser;
    private ExecutorService executorService;
    private Map<String, Integer> failuresPerHost = new HashMap<>();

    @Override
    public void setup(Context context) {
      super.setup(context);

      Configuration conf = context.getConfiguration();

      protocolFactory = new ProtocolFactory(conf);

      // SiteMapParser to allow "cross submits" from different prefixes
      // (up to last slash), cf. http://www.sitemaps.org/protocol.html#location
      // strict = true : do not allow cross submits.
      // This would need to pass a set of cross-submit allowed hosts beforehand
      // which is not supported by the sitemap parser. Done in SitemapInjector,
      // see below.
      boolean strict = conf.getBoolean("db.injector.sitemap.strict", false);
      sitemapParser = new SiteMapParser(strict, true);
      sitemapParser.setStrictNamespace(true);
      sitemapParser.addAcceptedNamespace(
          crawlercommons.sitemaps.Namespace.SITEMAP_LEGACY);
      sitemapParser
          .addAcceptedNamespace(crawlercommons.sitemaps.Namespace.NEWS);
      sitemapParser
          .addAcceptedNamespace(crawlercommons.sitemaps.Namespace.EMPTY);

      maxRecursiveSitemaps = conf.getInt("db.injector.sitemap.index_max_size",
          50001);
      maxRecursiveSitemapDepth = conf
          .getInt("db.injector.sitemap.index_max_depth", 3);
      maxUrlsPerSitemapIndex = conf.getLong(SITEMAP_MAX_URLS, 50000L * 50000);
      maxHostsPerSitemapIndex = conf.getInt(SITEMAP_MAX_HOSTS, 100);

      checkRobotsTxt = conf.getBoolean("db.injector.sitemap.checkrobotstxt",
          true);
      checkCrossSubmits = conf.getBoolean(SITEMAP_CROSS_SUBMIT_CHECK, true);
      checkCrossSubmitsType = CrossSubmitType
          .valueOf(conf.get(SITEMAP_CROSS_SUBMIT_CHECK_TYPE, "PRIVATE_DOMAIN"));

      // make sure a sitemap is entirely, even recursively processed within 80%
      // of the task timeout, do not start processing a subsitemap if fetch
      // and parsing time may hit the task timeout
      int taskTimeout = conf.getInt("mapreduce.task.timeout", 900000) / 1000;
      maxSitemapFetchTime = (int) (conf.getInt("http.time.limit", 120) * 1.5);
      maxSitemapProcessingTime = taskTimeout - (2 * maxSitemapFetchTime);
      if ((taskTimeout * .8) < maxSitemapProcessingTime) {
        maxSitemapProcessingTime = (int) (taskTimeout * .8);
      }
      maxFailuresPerHost = conf
          .getInt("db.injector.sitemap.max.fetch.failures.per.host", 5);

      maxRedirect = conf.getInt("db.injector.sitemap.max.redirect", 3);

      // fetch intervals defined in sitemap should within the defined range
      minInterval = conf.getFloat("db.fetch.schedule.adaptive.min_interval",
          60);
      maxInterval = conf.getFloat("db.fetch.schedule.max_interval",
          365 * 24 * 3600);
      if (maxInterval > conf.getInt("db.fetch.interval.max", 365 * 24 * 3600)) {
        maxInterval = conf.getInt("db.fetch.interval.max", 365 * 24 * 3600);
      }

      // Sitemaps can be quite large, so it is desirable to
      // increase the content limits above defaults (64kB):
      // TODO: make configurable?
      String[] contentLimitProperties = { "http.content.limit",
          "ftp.content.limit", "file.content.limit" };
      for (int i = 0; i < contentLimitProperties.length; i++) {
        conf.setInt(contentLimitProperties[i], SiteMapParser.MAX_BYTES_ALLOWED);
      }

      executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
          .setNameFormat("sitemapinj-%d").setDaemon(true).build());
    }

    public void map(Text key, Writable value, Context context)
        throws IOException, InterruptedException {

      // one line in the text file (= one sitemap URL):
      // - key is the first field,
      // - metadata is contained in value
      String url = key.toString().trim();
      if (url.isEmpty() || url.startsWith("#")) {
        // skip empty URLs or comment lines starting with '#'
        return;
      }

      float customScore = 0.0f;
      long maxUrls = maxUrlsPerSitemapIndex;
      int maxHosts = maxHostsPerSitemapIndex;
      Set<String> crossSubmits = new HashSet<>();
      Metadata customMetadata = new Metadata();
      String metadata = value.toString().trim();
      if (metadata.length() > 0) {
        String[] splits = metadata.split("[\t ]");
        for (String split : splits) {
          int indexEquals = split.indexOf("=");
          if (indexEquals == -1)
            continue;
          String metaname = split.substring(0, indexEquals);
          String metavalue = split.substring(indexEquals + 1);
          if (metaname.equals(nutchScoreMDName)) {
            try {
              customScore = Float.parseFloat(metavalue);
            } catch (NumberFormatException nfe) {
              LOG.error("Invalid custom score for sitemap seed {}: {} - {}",
                  url, metavalue, nfe.getMessage());
            }
          } else if (metaname.equals(SITEMAP_MAX_URLS)) {
            try {
              maxUrls = Long.parseLong(metavalue);
              LOG.info("Setting max. number of URLs per sitemap for {} = {}",
                  url, maxUrls);
            } catch (NumberFormatException nfe) {
              LOG.error("Invalid URL limit for sitemap seed {}: {} - {}", url,
                  metavalue, nfe.getMessage());
            }
          } else if (metaname.equals(SITEMAP_MAX_HOSTS)) {
            try {
              maxHosts = Integer.parseInt(metavalue);
              LOG.info("Setting max. number of hosts per sitemap for {} = {}",
                  url, maxHosts);
            } catch (NumberFormatException nfe) {
              LOG.error("Invalid host limit for sitemap seed {}: {} - {}", url,
                  metavalue, nfe.getMessage());
            }
          } else if (metaname.equals(SITEMAP_CROSS_SUBMITS)
              && checkCrossSubmits) {
            for (String target : metavalue.split(",")) {
              crossSubmits.add(target);
            }
          } else {
            customMetadata.add(metaname, metavalue);
          }
        }
      }

      SitemapProcessor sp = new SitemapProcessor(context, customScore, maxUrls,
          maxHosts, crossSubmits);
      sp.process(url);
    }

    class FetchSitemapCallable implements Callable<ProtocolOutput> {
      private Protocol protocol;
      private String url;
      private Context context;

      public FetchSitemapCallable(Protocol protocol, String url,
          Context context) {
        this.protocol = protocol;
        this.url = url;
        this.context = context;
      }

      @Override
      public ProtocolOutput call() throws Exception {
        Text turl = new Text(url);
        if (checkRobotsTxt) {
          BaseRobotRules rules = protocol.getRobotRules(turl, null, null);
          if (!rules.isAllowed(url)) {
            LOG.info("Fetch of sitemap forbidden by robots.txt: {}", url);
            context
                .getCounter("SitemapInjector",
                    "failed to fetch sitemap content, robots.txt disallow")
                .increment(1);
            return null;
          }
        }
        return protocol.getProtocolOutput(turl, new CrawlDatum());
      }
    }

    class ParseSitemapCallable implements Callable<AbstractSiteMap> {
      private Content content;
      private String url;
      private AbstractSiteMap sitemap;

      public ParseSitemapCallable(Content content, Object urlOrSitemap) {
        this.content = content;
        if (urlOrSitemap instanceof String)
          this.url = (String) urlOrSitemap;
        else if (urlOrSitemap instanceof AbstractSiteMap)
          this.sitemap = (AbstractSiteMap) urlOrSitemap;
        else
          throw new IllegalArgumentException(
              "URL (String) or sitemap (AbstractSiteMap) required as argument");
      }

      @Override
      public AbstractSiteMap call() throws Exception {
        if (sitemap != null) {
          return sitemapParser.parseSiteMap(content.getContentType(),
              content.getContent(), sitemap);
        } else {
          return sitemapParser.parseSiteMap(content.getContentType(),
              content.getContent(), new URL(url));
        }
      }
    }

    class ScoredSitemap implements Comparable<ScoredSitemap> {
      double score;
      AbstractSiteMap sitemap;

      public ScoredSitemap(double score, AbstractSiteMap sitemap) {
        this.score = score;
        this.sitemap = sitemap;
      }

      @Override
      public int compareTo(ScoredSitemap other) {
        return Double.compare(other.score, this.score);
      }
    }

    private void incrementFailuresPerHost(String hostName) {
      int failures = 1;
      if (failuresPerHost.containsKey(hostName)) {
        failures += failuresPerHost.get(hostName);
      }
      failuresPerHost.put(hostName, failures);
    }

    /** Wrapper for (recursively) fetching and parsing a sitemap */
    class SitemapProcessor {
      Context context;
      float customScore;
      long maxUrls;
      int maxHosts;

      long startTime = System.currentTimeMillis();
      long totalUrls = 0;
      Set<String> injectedHosts = new HashSet<>();
      Set<String> crossSubmits;

      public SitemapProcessor(Context context, float customScore, long maxUrls,
          int maxHosts, Set<String> crossSubmits) {
        this.context = context;
        this.maxUrls = maxUrls;
        this.maxHosts = maxHosts;
        this.crossSubmits = crossSubmits;

        // distribute site score to outlinks
        // TODO: should be by real number of outlinks not the maximum allowed
        customScore /= maxUrls;
        this.customScore = customScore;
      }

      /**
       * Within limited time: parse and process a sitemap (recursively, in case
       * of a sitemap index) and inject URLs
       */
      public void process(String url) {
        Content content = getContent(url);
        if (content == null) {
          return;
        }

        AbstractSiteMap sitemap = null;
        try {
          sitemap = parseSitemap(content, url);
        } catch (Exception e) {
          context.getCounter("SitemapInjector", "sitemaps failed to parse")
              .increment(1);
          LOG.warn("failed to parse sitemap {}: {}", url,
              StringUtils.stringifyException(e));
          return;
        }
        LOG.info("parsed sitemap {} ({})", url, sitemap.getType());

        if (checkCrossSubmits) {
          String host = sitemap.getUrl().getHost();
          String crossSubmit = host;
          if (checkCrossSubmitsType == CrossSubmitType.PRIVATE_DOMAIN) {
            crossSubmit = EffectiveTldFinder.getAssignedDomain(host, false,
                false);
          } else if (checkCrossSubmitsType == CrossSubmitType.PUBLIC_DOMAIN) {
            crossSubmit = EffectiveTldFinder.getAssignedDomain(host, false,
                true);
          }
          if (crossSubmit != null) {
            crossSubmits.add(crossSubmit);
          }
        }

        try {
          processSitemap(sitemap, null, 0);
        } catch (IOException | InterruptedException e) {
          LOG.warn("failed to process sitemap {}: {}", url,
              StringUtils.stringifyException(e));
        }
        LOG.info("Injected total {} URLs for {}", totalUrls, url);

      }

      /**
       * Parse a sitemap and inject all contained URLs. In case of a sitemap
       * index, sitemaps are fetched and processed recursively until one of the
       * configurable limits apply:
       * <ul>
       * <li>max. depth (<code>db.injector.sitemap.index_max_depth</code>)</li>
       * <li>max. processing time (recursively, depends on
       * <code>mapreduce.task.timeout</code>)</li>
       * <li>no URLs found at 50% of the processing time</li>
       * <li>max. number of recursive sitemaps
       * (<code>db.injector.sitemap.index_max_size</code>)</li>
       * <li>50% of the max. number of recursive sitemaps failed to process</li>
       * <li>max. number of URLs for this sitemap
       * (<code>db.injector.sitemap.max_urls</code>)</li>
       * </ul>
       * 
       * Subsitemaps from a sitemap index are selected randomly but giving
       * precedence to sitemaps recently published or coming in front of the
       * list of subsitemaps.
       * 
       * @param sitemap
       *          the sitemap to process
       * @param processedSitemaps
       *          set of recursively processed sitemaps, required to skip
       *          duplicates and to apply limits
       * @param depth
       *          the current depth when processing sitemaps recursively
       * @throws IOException
       * @throws InterruptedException
       */
      public void processSitemap(AbstractSiteMap sitemap,
          Set<String> processedSitemaps, int depth)
          throws IOException, InterruptedException {

        if (sitemap.isIndex()) {
          SiteMapIndex sitemapIndex = (SiteMapIndex) sitemap;
          if (processedSitemaps == null) {
            processedSitemaps = new HashSet<String>();
            processedSitemaps.add(sitemap.getUrl().toString());
          }
          if (++depth > maxRecursiveSitemapDepth) {
            LOG.warn(
                "Depth limit reached recursively processing sitemap index {}",
                sitemap.getUrl());
            context.getCounter("SitemapInjector",
                "sitemap index: depth limit reached").increment(1);
            return;
          }

          // choose subsitemaps randomly with a preference for elements in front
          // and recently published sitemaps
          PriorityQueue<ScoredSitemap> sitemaps = new PriorityQueue<>();
          int subSitemaps = 0;
          for (AbstractSiteMap s : sitemapIndex.getSitemaps()) {
            subSitemaps++;
            double publishScore = 0.3;
            if (s.getLastModified() != null) {
              double elapsedMonthsSincePublished = (System.currentTimeMillis()
                  - s.getLastModified().getTime())
                  / (1000.0 * 60 * 60 * 24 * 30);
              publishScore = (1.0
                  / Math.log(1.0 + elapsedMonthsSincePublished));
            }
            double score = (1.0 / subSitemaps) + publishScore + Math.random();
            sitemaps.add(new ScoredSitemap(score, s));
          }

          int failedSubSitemaps = 0;
          while (sitemaps.size() > 0) {

            long elapsed = (System.currentTimeMillis() - startTime) / 1000;
            if (elapsed > maxSitemapProcessingTime) {
              LOG.warn(
                  "Max. processing time reached, skipped remaining sitemaps of sitemap index {}",
                  sitemap.getUrl());
              context.getCounter("SitemapInjector",
                  "sitemap index: time limit reached").increment(1);
              return;
            }
            if ((totalUrls == 0)
                && (elapsed > (maxSitemapProcessingTime / 2))) {
              LOG.warn(
                  "Half of processing time elapsed and no URLs injected, skipped remaining sitemaps of sitemap index {}",
                  sitemap.getUrl());
              context
                  .getCounter("SitemapInjector",
                      "sitemap index: no URLs after 50% of time limit")
                  .increment(1);
              return;
            }
            if (failedSubSitemaps > (maxRecursiveSitemaps / 2)) {
              // do not spend too much time to fetch broken subsitemaps
              LOG.warn(
                  "Too many failures, skipped remaining sitemaps of sitemap index {}",
                  sitemap.getUrl());
              context.getCounter("SitemapInjector",
                  "sitemap index: too many failures").increment(1);
              return;
            }

            AbstractSiteMap nextSitemap = sitemaps.poll().sitemap;
            context.getCounter("SitemapInjector", "sitemap indexes processed")
                .increment(1);

            String url = nextSitemap.getUrl().toString();
            if (processedSitemaps.contains(url)) {
              LOG.warn("skipped recursive sitemap URL {}", url);
              context.getCounter("SitemapInjector",
                  "skipped recursive sitemap URLs").increment(1);
              nextSitemap.setProcessed(true);
              continue;
            }
            if (processedSitemaps.size() > maxRecursiveSitemaps) {
              LOG.warn(
                  "{} sitemaps processed for {}, skipped remaining sitemaps",
                  processedSitemaps.size(), sitemap.getUrl());
              context
                  .getCounter("SitemapInjector", "sitemap index limit reached")
                  .increment(1);
              return;
            }
            if (totalUrls >= maxUrls) {
              LOG.warn(
                  "URL limit reached, skipped remaining sitemaps of sitemap index {}",
                  sitemap.getUrl());
              context.getCounter("SitemapInjector",
                  "sitemap index: URL limit reached").increment(1);
              return;
            }

            processedSitemaps.add(url);

            Content content = getContent(url);
            if (content == null) {
              nextSitemap.setProcessed(true);
              context.getCounter("SitemapInjector", "sitemaps failed to fetch")
                  .increment(1);
              failedSubSitemaps++;
              continue;
            }

            try {
              AbstractSiteMap parsedSitemap = parseSitemap(content,
                  nextSitemap);
              processSitemap(parsedSitemap, processedSitemaps, depth);
            } catch (Exception e) {
              LOG.warn("failed to parse sitemap {}: {}", nextSitemap.getUrl(),
                  StringUtils.stringifyException(e));
              context.getCounter("SitemapInjector", "sitemaps failed to parse")
                  .increment(1);
              failedSubSitemaps++;
            }
            nextSitemap.setProcessed(true);
          }

        } else {
          context.getCounter("SitemapInjector", "sitemaps processed")
              .increment(1);
          injectURLs((SiteMap) sitemap);
          if (totalUrls >= maxUrls) {
            LOG.warn("URL limit reached, skipped remaining urls of {}",
                sitemap.getUrl());
            context.getCounter("SitemapInjector",
                "sitemap index: URL limit reached").increment(1);
            return;
          }
          sitemap.setProcessed(true);
        }
      }

      private Content getContent(String url) {
        if (url.length() > maxUrlLength) {
          LOG.warn(
              "Not fetching sitemap with overlong URL: {} ... (truncated, length = {} characters)",
              url.substring(0, maxUrlLength), url.length());
          context.getCounter("SitemapInjector", "sitemap overlong URL")
              .increment(1);
          return null;
        }
        String origUrl = url;
        url = filterNormalize(url);
        if (url == null) {
          LOG.warn("Sitemap rejected by URL filters: {}", origUrl);
          context
              .getCounter("SitemapInjector", "sitemap rejected by URL filters")
              .increment(1);
          return null;
        }
        String hostName;
        try {
          hostName = new URL(url).getHost();
        } catch (MalformedURLException e) {
          return null;
        }
        if (failuresPerHost.containsKey(hostName)
            && failuresPerHost.get(hostName) > maxFailuresPerHost) {
          LOG.info("Skipped, too many failures per host: {}", url);
          context.getCounter("SitemapInjector",
              "skipped, too many failures per host").increment(1);
          return null;
        }
        Protocol protocol = null;
        try {
          protocol = protocolFactory.getProtocol(url);
        } catch (ProtocolNotFound e) {
          LOG.error("protocol not found " + url);
          context
              .getCounter("SitemapInjector",
                  "failed to fetch sitemap content, protocol not found")
              .increment(1);
          return null;
        }

        LOG.info("fetching sitemap " + url);
        ProtocolOutput protocolOutput = null;
        origUrl = url;
        int redirects = 0;
        do {
          if (redirects > 0) {
            LOG.info("fetching redirected sitemap " + url);
          }
          FetchSitemapCallable fetch = new FetchSitemapCallable(protocol, url,
              context);
          Future<ProtocolOutput> task = executorService.submit(fetch);
          try {
            protocolOutput = task.get(maxSitemapFetchTime, TimeUnit.SECONDS);
          } catch (Exception e) {
            if (e instanceof TimeoutException) {
              LOG.error("fetch of sitemap {} timed out", url);
              context.getCounter("SitemapInjector",
                  "failed to fetch sitemap content, timeout").increment(1);
            } else {
              LOG.error("fetch of sitemap {} failed with: {}", url,
                  StringUtils.stringifyException(e));
              context
                  .getCounter("SitemapInjector",
                      "failed to fetch sitemap content, exception")
                  .increment(1);
            }
            task.cancel(true);
            incrementFailuresPerHost(hostName);
            return null;
          } finally {
            fetch = null;
          }

          if (protocolOutput == null) {
            return null;
          }

          if (protocolOutput.getStatus().isRedirect()) {
            context.getCounter("SitemapInjector", "sitemap redirect")
                .increment(1);
            String redirUrl = protocolOutput.getStatus().getArgs()[0];
            url = filterNormalize(redirUrl);
            if (url == null) {
              LOG.info(
                  "Redirect target of sitemap {} rejected by URL filters: {}",
                  origUrl, redirUrl);
              context
                  .getCounter("SitemapInjector",
                      "sitemap (redirect target) rejected by URL filters")
                  .increment(1);
              return null;
            }
            // TODO: cross-submitting via redirects?
            // - dangerous: if a spammer redirects sitemaps
            // it would allow arbitrary domains
            // try {
            // String host = new URL(url).getHost();
            // String domain = EffectiveTldFinder.getAssignedDomain(host, true,
            // true);
            // crossSubmitDomains.add(domain);
            // } catch (MalformedURLException e) {
            // // should not happen, as URL already has been checked by
            // filters/normalizers
            // }
            redirects++;
            if (redirects >= maxRedirect) {
              LOG.warn("sitemap redirect limit exceeded: {}", origUrl);
              context.getCounter("SitemapInjector",
                  "sitemap redirect limit exceeded").increment(1);
              // return to avoid that exceeded redirects are counted twice
              // (also as non-success fetch status)
              return null;
            }
          }
        } while (protocolOutput.getStatus().isRedirect()
            && redirects < maxRedirect);

        if (!protocolOutput.getStatus().isSuccess()) {
          LOG.error("fetch of sitemap {} failed with status code {}", url,
              protocolOutput.getStatus().getCode());
          context
              .getCounter("SitemapInjector",
                  "failed to fetch sitemap content, HTTP status != 200")
              .increment(1);
          incrementFailuresPerHost(hostName);
          return null;
        }

        Content content = protocolOutput.getContent();
        if (content == null) {
          LOG.error("No content for {}, status: {}", url,
              protocolOutput.getStatus().getMessage());
          context
              .getCounter("SitemapInjector",
                  "failed to fetch sitemap content, empty content")
              .increment(1);
          incrementFailuresPerHost(hostName);
          return null;
        }
        return content;
      }

      private AbstractSiteMap parseSitemap(Content content, Object urlOrSitemap)
          throws Exception {
        ParseSitemapCallable parse = new ParseSitemapCallable(content,
            urlOrSitemap);
        Future<AbstractSiteMap> task = executorService.submit(parse);
        AbstractSiteMap sitemap = null;
        try {
          // not a recursive task, should be fast
          sitemap = task.get((1 + maxSitemapProcessingTime / 5),
              TimeUnit.SECONDS);
        } finally {
          parse = null;
        }
        return sitemap;
      }

      /**
       * Inject all URLs contained in one {@link SiteMap}.
       */
      public void injectURLs(SiteMap sitemap)
          throws IOException, InterruptedException {

        Collection<SiteMapURL> sitemapURLs = sitemap.getSiteMapUrls();
        if (sitemapURLs.size() == 0) {
          LOG.info("No URLs in sitemap {}", sitemap.getUrl());
          context.getCounter("SitemapInjector", "empty sitemap").increment(1);
          return;
        }
        LOG.info("Found {} URLs in {}", sitemapURLs.size(), sitemap.getUrl());

        // random selection of URLs in case the sitemap contains more than
        // accepted
        // TODO:
        // - for sitemap index: should be done over multiple sub-sitemaps
        // - need to consider that URLs may be filtered away
        // => use "reservoir sampling"
        // (https://en.wikipedia.org/wiki/Reservoir_sampling)
        Random random = null;
        float randomSelect = 0.0f;
        if (sitemapURLs.size() > (maxUrls - totalUrls)) {
          randomSelect = (maxUrls - totalUrls) / (.95f * sitemapURLs.size());
          if (randomSelect < 1.0f) {
            random = new Random();
          }
        }

        int crossSubmitsRejected = 0;
        int hostLimitRejected = 0;
        for (SiteMapURL siteMapURL : sitemapURLs) {

          if (totalUrls >= maxUrls) {
            context.getCounter("SitemapInjector", "sitemap URL limit reached")
                .increment(1);
            LOG.info("URL limit ({}) reached for {}", maxUrls,
                sitemap.getUrl());
            break;
          }

          if (random != null) {
            if (randomSelect > random.nextFloat()) {
              context.getCounter("SitemapInjector", "random skip").increment(1);
              continue;
            }
          }

          // TODO: score and fetch interval should be transparently overridable
          float sitemapScore = (float) siteMapURL.getPriority();
          sitemapScore *= customScore;
          int sitemapInterval = getChangeFrequencySeconds(
              siteMapURL.getChangeFrequency());
          long lastModified = -1;
          if (siteMapURL.getLastModified() != null) {
            lastModified = siteMapURL.getLastModified().getTime();
          }
          URL u = siteMapURL.getUrl();
          String url = u.toString();
          if (url.length() > maxUrlLength) {
            LOG.warn(
                "Skipping overlong URL: {} ... (truncated, length = {} characters)",
                url.substring(0, maxUrlLength), url.length());
            continue;
          }
          // for simplicity do host and domain checks before normalization
          String host = u.getHost();
          if (injectedHosts.size() >= maxHosts
              && !injectedHosts.contains(host)) {
            hostLimitRejected++;
            context
                .getCounter("SitemapInjector",
                    "urls from sitemaps rejected, host limit reached")
                .increment(1);
            continue;
          }
          if (checkCrossSubmits) {
            String crossSubmit = host;
            if (checkCrossSubmitsType == CrossSubmitType.PRIVATE_DOMAIN) {
              crossSubmit = EffectiveTldFinder.getAssignedDomain(host, false,
                  false);
            } else if (checkCrossSubmitsType == CrossSubmitType.PUBLIC_DOMAIN) {
              crossSubmit = EffectiveTldFinder.getAssignedDomain(host, false,
                  true);
            }
            if (crossSubmit == null || !crossSubmits.contains(crossSubmit)) {
              crossSubmitsRejected++;
              context.getCounter("SitemapInjector",
                  "urls from sitemaps rejected, target not allowed by cross-submits")
                  .increment(1);
              continue;
            }
          }
          try {
            url = filterNormalize(url);
          } catch (Exception e) {
            if (LOG.isWarnEnabled()) {
              LOG.warn("Skipping {}: {}", url,
                  StringUtils.stringifyException(e));
            }
            url = null;
          }
          if (url == null) {
            context.getCounter("SitemapInjector",
                "urls from sitemaps rejected by URL filters").increment(1);
          } else {
            // URL passed normalizers and filters
            totalUrls++;
            Text value = new Text(url);
            CrawlDatum datum = new CrawlDatum(CrawlDatum.STATUS_INJECTED,
                sitemapInterval, sitemapScore);
            if (lastModified != -1) {
              // datum.setModifiedTime(lastModified);
            }
            datum.setFetchTime(curTime);

            try {
              scfilters.injectedScore(value, datum);
            } catch (ScoringFilterException e) {
              if (LOG.isWarnEnabled()) {
                LOG.warn("Cannot filter injected score for url " + url
                    + ", using default (" + e.getMessage() + ")");
              }
            }

            context.getCounter("SitemapInjector", "urls from sitemaps injected")
                .increment(1);
            context.write(value, datum);
            injectedHosts.add(host);
          }
        }
        if (crossSubmitsRejected > 0) {
          LOG.info("Rejected {} cross-submits for {} ({})",
              crossSubmitsRejected, sitemap.getUrl(),
              sitemap.getType().toString());
        }
        if (hostLimitRejected > 0) {
          LOG.info(
              "Rejected {} URLs because max. number of linked hosts is reached for {} ({})",
              hostLimitRejected, sitemap.getUrl(),
              sitemap.getType().toString());
        }
      }

    }

    /**
     * Determine fetch schedule intervals based on given
     * <code>changefrequency</code> but adjusted to min. and max. intervals
     *
     * @param changeFrequency
     * @return interval in seconds
     */
    private int getChangeFrequencySeconds(
        SiteMapURL.ChangeFrequency changeFrequency) {
      float cf = interval;
      if (changeFrequency != null) {
        switch (changeFrequency) {
        case NEVER:
          cf = maxInterval;
          break;
        case YEARLY:
          cf = 365 * 24 * 3600;
          break;
        case MONTHLY:
          cf = 30 * 24 * 3600;
          break;
        case WEEKLY:
          cf = 7 * 24 * 3600;
          break;
        case DAILY:
          cf = 24 * 3600;
          break;
        case HOURLY:
          cf = 3600;
          break;
        case ALWAYS:
          cf = minInterval;
          break;
        }
      }
      if (cf < minInterval) {
        cf = minInterval;
      } else if (cf > maxInterval) {
        cf = maxInterval;
      }
      return (int) cf;
    }

  }

  public void inject(Path crawlDb, Path urlDir, boolean overwrite,
      boolean update, boolean normalize, boolean filter,
      boolean filterNormalizeAll)
      throws IOException, ClassNotFoundException, InterruptedException {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("SitemapInjector: starting at " + sdf.format(start));
      LOG.info("SitemapInjector: crawlDb: " + crawlDb);
      LOG.info("SitemapInjector: urlDir: " + urlDir);
      // for all sitemap URLs listed in text input file(s)
      // fetch and parse the sitemap, and map the contained URLs to
      // <url,CrawlDatum> pairs
      LOG.info(
          "SitemapInjector: Fetching sitemaps, injecting URLs from sitemaps to crawl db entries.");
    }

    // set configuration
    Configuration conf = getConf();
    conf.setLong("injector.current.time", System.currentTimeMillis());
    conf.setBoolean("db.injector.overwrite", overwrite);
    conf.setBoolean("db.injector.update", update);
    conf.setBoolean(CrawlDbFilter.URL_NORMALIZING, normalize);
    conf.setBoolean(CrawlDbFilter.URL_FILTERING, filter);
    conf.setBoolean(URL_FILTER_NORMALIZE_ALL, filterNormalizeAll);
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

    Path tempDir;
    Path lock = null;
    if (runStepOneOnly) {
      tempDir = crawlDb;
    } else {
      if (runStepTwoOnly) {
        tempDir = urlDir;
      } else {
        tempDir = new Path(getConf().get("mapreduce.cluster.temp.dir", ".")
            + "/sitemap-inject-temp-"
            + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
      }
      // lock an existing crawldb to prevent multiple simultaneous updates
      lock = CrawlDb.lock(conf, crawlDb, false);
    }

    if (!runStepTwoOnly) {
      Job sitemapJob = NutchJob.getInstance(getConf());
      sitemapJob.setJobName("process sitemaps " + urlDir);
      sitemapJob.setJarByClass(SitemapInjector.class);
      sitemapJob.setInputFormatClass(KeyValueTextInputFormat.class);
      KeyValueTextInputFormat.addInputPath(sitemapJob, urlDir);

      sitemapJob.setMapperClass(MultithreadedMapper.class);
      MultithreadedMapper.setMapperClass(sitemapJob, SitemapInjectMapper.class);
      MultithreadedMapper.setNumberOfThreads(sitemapJob, threads);
      sitemapJob.setMapSpeculativeExecution(false); // mappers are fetching
                                                    // sitemaps

      FileOutputFormat.setOutputPath(sitemapJob, tempDir);
      sitemapJob.setOutputFormatClass(SequenceFileOutputFormat.class);
      sitemapJob.setOutputKeyClass(Text.class);
      sitemapJob.setOutputValueClass(CrawlDatum.class);

      conf = sitemapJob.getConfiguration();
      conf.setLong("injector.current.time", System.currentTimeMillis());
      try {
        // run the job
        boolean success = sitemapJob.waitForCompletion(true);
        if (!success) {
          String message = "SitemapInjector job did not succeed, job status: "
              + sitemapJob.getStatus().getState() + ", reason: "
              + sitemapJob.getStatus().getFailureInfo();
          LOG.error(message);
          NutchJob.cleanupAfterFailure(tempDir, lock,
              tempDir.getFileSystem(conf));
          // throw exception so that calling routine can exit with error
          throw new RuntimeException(message);
        }
      } catch (IOException | InterruptedException | ClassNotFoundException
          | NullPointerException e) {
        LOG.error("SitemapInjector job failed: {}", e.getMessage());
        NutchJob.cleanupAfterFailure(tempDir, lock,
            tempDir.getFileSystem(conf));
        throw e;
      }

      for (Counter counter : sitemapJob.getCounters()
          .getGroup("SitemapInjector")) {
        LOG.info(String.format("SitemapInjector: %8d  %s", counter.getValue(),
            counter.getName()));
      }

      long end = System.currentTimeMillis();
      LOG.info("SitemapInjector: finished fetching and processing sitemaps at " + sdf.format(end) + ", elapsed: "
          + TimingUtil.elapsedTime(start, end));

      if (runStepOneOnly) {
        return;
      }

      long numOutputRecords = sitemapJob.getCounters()
          .findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
      if (numOutputRecords == 0) {
        LOG.warn(
            "No URLs found in sitemaps, skipping step 2 merging URLs into CrawlDb");
        return;
      }
    }

    // merge with existing CrawlDb
    if (LOG.isInfoEnabled()) {
      LOG.info("SitemapInjector: Merging injected urls into crawl db.");
    }
    Job mergeJob = CrawlDb.createJob(getConf(), crawlDb);
    FileInputFormat.addInputPath(mergeJob, tempDir);
    mergeJob.setReducerClass(InjectReducer.class);
    conf = mergeJob.getConfiguration();
    if (filterNormalizeAll) {
      conf.setBoolean(CrawlDbFilter.URL_FILTERING, filter);
      conf.setBoolean(CrawlDbFilter.URL_NORMALIZING, normalize);
    } else {
      conf.setBoolean(CrawlDbFilter.URL_FILTERING, false);
      conf.setBoolean(CrawlDbFilter.URL_NORMALIZING, false);
    }

    try {
      // run the job
      boolean success = mergeJob.waitForCompletion(true);
      if (!success) {
        String message = "SitemapInjector job did not succeed, job status: "
            + mergeJob.getStatus().getState() + ", reason: "
            + mergeJob.getStatus().getFailureInfo();
        LOG.error(message);
        NutchJob.cleanupAfterFailure(tempDir, lock,
            tempDir.getFileSystem(conf));
        // throw exception so that calling routine can exit with error
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException
        | NullPointerException e) {
      LOG.error("SitemapInjector job failed: {}", e.getMessage());
      NutchJob.cleanupAfterFailure(tempDir, lock, tempDir.getFileSystem(conf));
      throw e;
    }

    CrawlDb.install(mergeJob, crawlDb);

    // clean up
    if (!(keepTemp || runStepOneOnly || runStepTwoOnly)) {
      tempDir.getFileSystem(conf).delete(tempDir, true);
    }

    long end = System.currentTimeMillis();
    LOG.info("SitemapInjector: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  public void usage(String errorMessage) {
    System.err.println(errorMessage + "\n");
    usage();
  }

  public void usage() {
    System.err.println(
        "Usage: SitemapInjector [-D...] <crawldb> <url_dir> [-threads <n>] [-overwrite|-update] [-noFilter] [-noNormalize] [-filterNormalizeAll]\n");
    System.err.println("\nFor sitemap URLs listed in seed input files:");
    System.err.println("\t- fetch and parse the sitemap (step 1)");
    System.err.println("\t- inject URLs from sitemaps into the CrawlDb (step 2)");
    System.err.println(
        "\t- using fetch intervals and scores from sitemaps if applicable");
    System.err.println("Options and properties of SitemapInjector");
    System.err.println(
        "\t-threads <threads>\tNumber of threads created per mapper to fetch sitemap urls (default: 8)");
    System.err.println(
        "\t-keepTemp\tDo not delete the temporary directory which contains the output of step 1");
    System.err.println(
        "\t-step1\tOnly run step 1 (<crawldb> is used as output path and must not exist)");
    System.err.println(
        "\t-step2\tOnly run step 2 (<url_dir> must point to the output of step 1)");
    System.err.println(
        "\nIn addition, all options of Injector are supported, see below.\n");
    super.usage();
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new SitemapInjector(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      usage();
      return -1;
    }
    List<String> superArguments = new ArrayList<>();
    for (int i = 0; i < args.length; i++) {
      if (i < 2) {
        superArguments.add(args[i]);
        continue;
      }
      switch (args[i]) {
        case "-threads":
          i++;
          if (i == args.length) {
            usage("Argument -threads requires parameter");
            return -1;
          }
          threads = Integer.parseInt(args[i]);
          break;
        case "-keepTemp":
          keepTemp = true;
          break;
        case "-step1":
          runStepOneOnly = true;
          break;
        case "-step2":
          runStepTwoOnly = true;
          break;
        default:
          superArguments.add(args[i]);
      }
    }
    if (runStepOneOnly && runStepTwoOnly) {
      LOG.warn("Running step 1 and 2 as both -step1 and -step2 are defined.");
      runStepOneOnly = false;
      runStepTwoOnly = false;
      return -1;
    }
    return super.run(superArguments.toArray(new String[0]));
  }
}
