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
package org.apache.nutch.fetcher;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.fetcher.Fetcher.FetcherRun;
import org.apache.nutch.fetcher.FetcherThreadEvent.PublishEventType;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLExemptionFilters;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseOutputFormat;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.service.NutchServer;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crawlercommons.robots.BaseRobotRules;

/**
 * This class picks items from queues and fetches the pages.
 */
public class FetcherThread extends Thread {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration conf;
  private URLFilters urlFilters;
  private URLExemptionFilters urlExemptionFilters;
  private ScoringFilters scfilters;
  private ParseUtil parseUtil;
  private URLNormalizers normalizers;
  private ProtocolFactory protocolFactory;
  private long maxCrawlDelay;
  private String queueMode;
  private int maxRedirect;
  private String reprUrl;
  private boolean redirecting;
  private int redirectCount;
  private boolean ignoreInternalLinks;
  private boolean ignoreExternalLinks;
  private boolean ignoreAlsoRedirects;
  private String ignoreExternalLinksMode;

  // Used by fetcher.follow.outlinks.depth in parse
  private int maxOutlinksPerPage;
  private final int maxOutlinks;
  private final int interval;
  private int maxOutlinkDepth;
  private int maxOutlinkDepthNumLinks;
  private boolean outlinksIgnoreExternal;

  URLFilters urlFiltersForOutlinks;
  URLNormalizers normalizersForOutlinks;

  private boolean skipTruncated;

  private boolean halted = false;

  private AtomicInteger activeThreads;

  private Object fetchQueues;

  private QueueFeeder feeder;

  private Object spinWaiting;

  private AtomicLong lastRequestStart;

  private AtomicInteger errors;

  private String segmentName;

  private boolean parsing;

  private FetcherRun.Context context;

  private boolean storingContent;

  private AtomicInteger pages;

  private AtomicLong bytes;
  
  private List<Content> robotsTxtContent = null;

  //Used by the REST service
  private FetchNode fetchNode;
  private boolean reportToNutchServer;
  
  //Used for publishing events
  private FetcherThreadPublisher publisher;
  private boolean activatePublisher;

  public FetcherThread(Configuration conf, AtomicInteger activeThreads, FetchItemQueues fetchQueues, 
      QueueFeeder feeder, AtomicInteger spinWaiting, AtomicLong lastRequestStart, FetcherRun.Context context,
      AtomicInteger errors, String segmentName, boolean parsing, boolean storingContent, 
      AtomicInteger pages, AtomicLong bytes) {
    this.setDaemon(true); // don't hang JVM on exit
    this.setName("FetcherThread"); // use an informative name
    this.conf = conf;
    this.urlFilters = new URLFilters(conf);
    this.urlExemptionFilters = new URLExemptionFilters(conf);
    this.scfilters = new ScoringFilters(conf);
    this.parseUtil = new ParseUtil(conf);
    this.skipTruncated = conf.getBoolean(ParseSegment.SKIP_TRUNCATED, true);
    this.protocolFactory = new ProtocolFactory(conf);
    this.normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_FETCHER);
    this.maxCrawlDelay = conf.getInt("fetcher.max.crawl.delay", 30) * 1000;
    this.activeThreads = activeThreads;
    this.fetchQueues = fetchQueues;
    this.feeder = feeder;
    this.spinWaiting = spinWaiting;
    this.lastRequestStart = lastRequestStart;
    this.context = context;
    this.errors = errors;
    this.segmentName = segmentName;
    this.parsing = parsing;
    this.storingContent = storingContent;
    this.pages = pages;
    this.bytes = bytes;

    // NUTCH-2413 Apply filters and normalizers on outlinks
    // when parsing only if configured
    if (parsing) {
      if (conf.getBoolean("parse.filter.urls", true))
        this.urlFiltersForOutlinks = urlFilters;
      if (conf.getBoolean("parse.normalize.urls", true))
        this.normalizersForOutlinks = new URLNormalizers(conf,
            URLNormalizers.SCOPE_OUTLINK);
    }

    if((activatePublisher=conf.getBoolean("fetcher.publisher", false)))
      this.publisher = new FetcherThreadPublisher(conf);
    
    queueMode = conf.get("fetcher.queue.mode",
        FetchItemQueues.QUEUE_MODE_HOST);
    // check that the mode is known
    if (!queueMode.equals(FetchItemQueues.QUEUE_MODE_IP)
        && !queueMode.equals(FetchItemQueues.QUEUE_MODE_DOMAIN)
        && !queueMode.equals(FetchItemQueues.QUEUE_MODE_HOST)) {
      LOG.error("Unknown partition mode : {} - forcing to byHost", queueMode);
      queueMode = FetchItemQueues.QUEUE_MODE_HOST;
    }
    LOG.info("{} {} Using queue mode : {}", getName(),
        Thread.currentThread().getId(), queueMode);
    this.maxRedirect = conf.getInt("http.redirect.max", 3);

    maxOutlinksPerPage = conf.getInt("db.max.outlinks.per.page", 100);
    maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE
        : maxOutlinksPerPage;
    interval = conf.getInt("db.fetch.interval.default", 2592000);
    ignoreInternalLinks = conf.getBoolean("db.ignore.internal.links", false);
    ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);
    ignoreAlsoRedirects = conf.getBoolean("db.ignore.also.redirects", true);
    ignoreExternalLinksMode = conf.get("db.ignore.external.links.mode", "byHost");
    maxOutlinkDepth = conf.getInt("fetcher.follow.outlinks.depth", -1);
    outlinksIgnoreExternal = conf.getBoolean(
        "fetcher.follow.outlinks.ignore.external", false);
    maxOutlinkDepthNumLinks = conf.getInt(
        "fetcher.follow.outlinks.num.links", 4);
    if (conf.getBoolean("fetcher.store.robotstxt", false)) {
      if (storingContent) {
        robotsTxtContent = new LinkedList<>();
      } else {
        LOG.warn(
            "{} {} Ignoring fetcher.store.robotstxt because not storing content (fetcher.store.content)!",
            getName(), Thread.currentThread().getId());
      }
    }
  }

  @SuppressWarnings("fallthrough")
  public void run() {
    activeThreads.incrementAndGet(); // count threads

    FetchItem fit = null;
    try {
      // checking for the server to be running and fetcher.parse to be true
      if (parsing && NutchServer.getInstance().isRunning())
        reportToNutchServer = true;
      
      while (true) {
        // creating FetchNode for storing in FetchNodeDb
        if (reportToNutchServer)
          this.fetchNode = new FetchNode();
        else
          this.fetchNode = null;

        // check whether must be stopped
        if (isHalted()) {
          LOG.debug("{} set to halted", getName());
          fit = null;
          return;
        }

        fit = ((FetchItemQueues) fetchQueues).getFetchItem();
        if (fit == null) {
          if (feeder.isAlive() || ((FetchItemQueues) fetchQueues).getTotalSize() > 0) {
            LOG.debug("{} spin-waiting ...", getName());
            // spin-wait.
            ((AtomicInteger) spinWaiting).incrementAndGet();
            try {
              Thread.sleep(500);
            } catch (Exception e) {
            }
            ((AtomicInteger) spinWaiting).decrementAndGet();
            continue;
          } else {
            // all done, finish this thread
            LOG.info("{} {} has no more work available", getName(),
                Thread.currentThread().getId());
            return;
          }
        }
        lastRequestStart.set(System.currentTimeMillis());

        Text reprUrlWritable = (Text) fit.datum.getMetaData().get(
            Nutch.WRITABLE_REPR_URL_KEY);
        if (reprUrlWritable == null) {
          setReprUrl(fit.url.toString());
        } else {
          setReprUrl(reprUrlWritable.toString());
        }

        try {
          // fetch the page
          redirecting = false;
          redirectCount = 0;
          
          //Publisher event
          if(activatePublisher) {
            FetcherThreadEvent startEvent = new FetcherThreadEvent(PublishEventType.START, fit.getUrl().toString());
            publisher.publish(startEvent, conf);
          }
          
          do {
            if (LOG.isInfoEnabled()) {
              LOG.info("{} {} fetching {} (queue crawl delay={}ms)", getName(),
                  Thread.currentThread().getId(), fit.url,
                  ((FetchItemQueues) fetchQueues)
                      .getFetchItemQueue(fit.queueID).crawlDelay);
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("redirectCount={}", redirectCount);
            }
            redirecting = false;
            Protocol protocol = this.protocolFactory.getProtocol(fit.url
                .toString());
            BaseRobotRules rules = protocol.getRobotRules(fit.url, fit.datum, robotsTxtContent);
            if (robotsTxtContent != null) {
              outputRobotsTxt(robotsTxtContent);
              robotsTxtContent.clear();
            }
            if (!rules.isAllowed(fit.u.toString())) {
              // unblock
              ((FetchItemQueues) fetchQueues).finishFetchItem(fit, true);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Denied by robots.txt: {}", fit.url);
              }
              output(fit.url, fit.datum, null,
                  ProtocolStatus.STATUS_ROBOTS_DENIED,
                  CrawlDatum.STATUS_FETCH_GONE);
              context.getCounter("FetcherStatus", "robots_denied").increment(1);
              continue;
            }
            if (rules.getCrawlDelay() > 0) {
              if (rules.getCrawlDelay() > maxCrawlDelay && maxCrawlDelay >= 0) {
                // unblock
                ((FetchItemQueues) fetchQueues).finishFetchItem(fit, true);
                LOG.debug("Crawl-Delay for {} too long ({}), skipping", fit.url,
                    rules.getCrawlDelay());
                output(fit.url, fit.datum, null,
                    ProtocolStatus.STATUS_ROBOTS_DENIED,
                    CrawlDatum.STATUS_FETCH_GONE);
                context.getCounter("FetcherStatus",
                    "robots_denied_maxcrawldelay").increment(1);
                continue;
              } else {
                FetchItemQueue fiq = ((FetchItemQueues) fetchQueues)
                    .getFetchItemQueue(fit.queueID);
                fiq.crawlDelay = rules.getCrawlDelay();
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Crawl delay for queue: " + fit.queueID
                      + " is set to " + fiq.crawlDelay
                      + " as per robots.txt. url: " + fit.url);
                }
              }
            }
            ProtocolOutput output = protocol.getProtocolOutput(fit.url,
                fit.datum);
            ProtocolStatus status = output.getStatus();
            Content content = output.getContent();
            ParseStatus pstatus = null;
            // unblock queue
            ((FetchItemQueues) fetchQueues).finishFetchItem(fit);

            // used for FetchNode
            if (fetchNode != null) {
              fetchNode.setStatus(status.getCode());
              fetchNode.setFetchTime(System.currentTimeMillis());
              fetchNode.setUrl(fit.url);
            }
            
            //Publish fetch finish event
            if(activatePublisher) {
              FetcherThreadEvent endEvent = new FetcherThreadEvent(PublishEventType.END, fit.getUrl().toString());
              endEvent.addEventData("status", status.getName());
              publisher.publish(endEvent, conf);
            }
            context.getCounter("FetcherStatus", status.getName()).increment(1);

            switch (status.getCode()) {

            case ProtocolStatus.WOULDBLOCK:
              // retry ?
              ((FetchItemQueues) fetchQueues).addFetchItem(fit);
              break;

            case ProtocolStatus.SUCCESS: // got a page
              pstatus = output(fit.url, fit.datum, content, status,
                  CrawlDatum.STATUS_FETCH_SUCCESS, fit.outlinkDepth);
              updateStatus(content.getContent().length);
              if (pstatus != null && pstatus.isSuccess()
                  && pstatus.getMinorCode() == ParseStatus.SUCCESS_REDIRECT) {
                String newUrl = pstatus.getMessage();
                int refreshTime = Integer.valueOf(pstatus.getArgs()[1]);
                Text redirUrl = handleRedirect(fit, newUrl,
                    refreshTime < Fetcher.PERM_REFRESH_TIME,
                    Fetcher.CONTENT_REDIR);
                if (redirUrl != null) {
                  fit = queueRedirect(redirUrl, fit);
                }
              }
              break;

            case ProtocolStatus.MOVED: // redirect
            case ProtocolStatus.TEMP_MOVED:
              int code;
              boolean temp;
              if (status.getCode() == ProtocolStatus.MOVED) {
                code = CrawlDatum.STATUS_FETCH_REDIR_PERM;
                temp = false;
              } else {
                code = CrawlDatum.STATUS_FETCH_REDIR_TEMP;
                temp = true;
              }
              output(fit.url, fit.datum, content, status, code);
              String newUrl = status.getMessage();
              Text redirUrl = handleRedirect(fit, newUrl, temp,
                  Fetcher.PROTOCOL_REDIR);
              if (redirUrl != null) {
                fit = queueRedirect(redirUrl, fit);
              } else {
                // stop redirecting
                redirecting = false;
              }
              break;

            case ProtocolStatus.EXCEPTION:
              logError(fit.url, status.getMessage());
              int killedURLs = ((FetchItemQueues) fetchQueues).checkExceptionThreshold(fit
                  .getQueueID());
              if (killedURLs != 0)
                context.getCounter("FetcherStatus",
                    "AboveExceptionThresholdInQueue").increment(killedURLs);
              /* FALLTHROUGH */
            case ProtocolStatus.RETRY: // retry
            case ProtocolStatus.BLOCKED:
              output(fit.url, fit.datum, null, status,
                  CrawlDatum.STATUS_FETCH_RETRY);
              break;

            case ProtocolStatus.GONE: // gone
            case ProtocolStatus.NOTFOUND:
            case ProtocolStatus.ACCESS_DENIED:
            case ProtocolStatus.ROBOTS_DENIED:
              output(fit.url, fit.datum, null, status,
                  CrawlDatum.STATUS_FETCH_GONE);
              break;

            case ProtocolStatus.NOTMODIFIED:
              output(fit.url, fit.datum, null, status,
                  CrawlDatum.STATUS_FETCH_NOTMODIFIED);
              break;

            default:
              if (LOG.isWarnEnabled()) {
                LOG.warn("{} {} Unknown ProtocolStatus: {}", getName(),
                    Thread.currentThread().getId(), status.getCode());
              }
              output(fit.url, fit.datum, null, status,
                  CrawlDatum.STATUS_FETCH_RETRY);
            }

            if (redirecting && redirectCount > maxRedirect) {
              ((FetchItemQueues) fetchQueues).finishFetchItem(fit);
              if (LOG.isInfoEnabled()) {
                LOG.info("{} {} - redirect count exceeded {}", getName(),
                    Thread.currentThread().getId(), fit.url);
              }
              output(fit.url, fit.datum, null,
                  ProtocolStatus.STATUS_REDIR_EXCEEDED,
                  CrawlDatum.STATUS_FETCH_GONE);
            }

          } while (redirecting && (redirectCount <= maxRedirect));

        } catch (Throwable t) { // unexpected exception
          // unblock
          ((FetchItemQueues) fetchQueues).finishFetchItem(fit);
          logError(fit.url, StringUtils.stringifyException(t));
          output(fit.url, fit.datum, null, ProtocolStatus.STATUS_FAILED,
              CrawlDatum.STATUS_FETCH_RETRY);
        }
      }

    } catch (Throwable e) {
      if (LOG.isErrorEnabled()) {
        LOG.error("fetcher caught:", e);
      }
    } finally {
      if (fit != null)
        ((FetchItemQueues) fetchQueues).finishFetchItem(fit);
      activeThreads.decrementAndGet(); // count threads
      LOG.info("{} {} -finishing thread {}, activeThreads={}", getName(),
          Thread.currentThread().getId(), getName(), activeThreads);
    }
  }

  private Text handleRedirect(FetchItem fit, String newUrl,
      boolean temp, String redirType)
      throws MalformedURLException, URLFilterException, InterruptedException {
    newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
    newUrl = urlFilters.filter(newUrl);
    String urlString = fit.url.toString();

    if (newUrl == null || newUrl.equals(urlString)) {
      LOG.debug(" - {} redirect skipped: {}", redirType,
          (newUrl != null ? "to same url" : "filtered"));
      return null;
    }

    if (ignoreAlsoRedirects && (ignoreExternalLinks || ignoreInternalLinks)) {
      try {
        URL origUrl = fit.u;
        URL redirUrl = new URL(newUrl);
        if (ignoreExternalLinks) {
          String origHostOrDomain, newHostOrDomain;
          if ("bydomain".equalsIgnoreCase(ignoreExternalLinksMode)) {
            origHostOrDomain = URLUtil.getDomainName(origUrl).toLowerCase();
            newHostOrDomain = URLUtil.getDomainName(redirUrl).toLowerCase();
          } else {
            // byHost
            origHostOrDomain = origUrl.getHost().toLowerCase();
            newHostOrDomain = redirUrl.getHost().toLowerCase();
          }
          if (!origHostOrDomain.equals(newHostOrDomain)) {
            LOG.debug(
                " - ignoring redirect {} from {} to {} because external links are ignored",
                redirType, urlString, newUrl);
            return null;
          }
        }

        if (ignoreInternalLinks) {
          String origHost = origUrl.getHost().toLowerCase();
          String newHost = redirUrl.getHost().toLowerCase();
          if (origHost.equals(newHost)) {
            LOG.debug(
                " - ignoring redirect {} from {} to {} because internal links are ignored",
                redirType, urlString, newUrl);
            return null;
          }
        }
      } catch (MalformedURLException e) {
        return null;
      }
    }

    reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temp);
    Text url = new Text(newUrl);
    if (maxRedirect > 0) {
      redirecting = true;
      redirectCount++;
      LOG.debug(" - {} redirect to {} (fetching now)", redirType, url);
      return url;
    } else {
      CrawlDatum newDatum = new CrawlDatum(CrawlDatum.STATUS_LINKED,
          fit.datum.getFetchInterval(), fit.datum.getScore());
      // transfer existing metadata
      newDatum.getMetaData().putAll(fit.datum.getMetaData());
      try {
        scfilters.initialScore(url, newDatum);
      } catch (ScoringFilterException e) {
        e.printStackTrace();
      }
      if (reprUrl != null) {
        newDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY,
            new Text(reprUrl));
      }
      output(url, newDatum, null, null, CrawlDatum.STATUS_LINKED);
      LOG.debug(" - {} redirect to {} (fetching later)", redirType, url);
      return null;
    }
  }

  private FetchItem queueRedirect(Text redirUrl, FetchItem fit)
      throws ScoringFilterException {
    CrawlDatum newDatum = new CrawlDatum(CrawlDatum.STATUS_DB_UNFETCHED,
        fit.datum.getFetchInterval(), fit.datum.getScore());
    // transfer all existing metadata to the redirect
    newDatum.getMetaData().putAll(fit.datum.getMetaData());
    scfilters.initialScore(redirUrl, newDatum);
    if (reprUrl != null) {
      newDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY,
          new Text(reprUrl));
    }
    fit = FetchItem.create(redirUrl, newDatum, queueMode);
    if (fit != null) {
      FetchItemQueue fiq = ((FetchItemQueues) fetchQueues).getFetchItemQueue(fit.queueID);
      fiq.addInProgressFetchItem(fit);
    } else {
      // stop redirecting
      redirecting = false;
      context.getCounter("FetcherStatus", "FetchItem.notCreated.redirect").increment(1);
    }
    return fit;
  }

  private void logError(Text url, String message) {
    if (LOG.isInfoEnabled()) {
      LOG.info("{} {} fetch of {} failed with: ", getName(),
          Thread.currentThread().getId(), url, message);
    }
    errors.incrementAndGet();
  }

  private ParseStatus output(Text key, CrawlDatum datum, Content content,
      ProtocolStatus pstatus, int status) throws InterruptedException{
    return output(key, datum, content, pstatus, status, 0);
  }

  private ParseStatus output(Text key, CrawlDatum datum, Content content,
      ProtocolStatus pstatus, int status, int outlinkDepth) throws InterruptedException{

    datum.setStatus(status);
    datum.setFetchTime(System.currentTimeMillis());
    if (pstatus != null)
      datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY, pstatus);

    ParseResult parseResult = null;
    if (content != null) {
      Metadata metadata = content.getMetadata();

      // store the guessed content type in the crawldatum
      if (content.getContentType() != null)
        datum.getMetaData().put(new Text(Metadata.CONTENT_TYPE),
            new Text(content.getContentType()));

      // add segment to metadata
      metadata.set(Nutch.SEGMENT_NAME_KEY, segmentName);
      // add score to content metadata so that ParseSegment can pick it up.
      try {
        scfilters.passScoreBeforeParsing(key, datum, content);
      } catch (Exception e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("{} {} Couldn't pass score, url {} ({})", getName(),
              Thread.currentThread().getId(), key, e);
        }
      }
      /*
       * Note: Fetcher will only follow meta-redirects coming from the
       * original URL.
       */
      if (parsing && status == CrawlDatum.STATUS_FETCH_SUCCESS) {
        if (!skipTruncated
            || (skipTruncated && !ParseSegment.isTruncated(content))) {
          try {
            parseResult = this.parseUtil.parse(content);
          } catch (Exception e) {
            LOG.warn("{} {} Error parsing: {}: {}", getName(),
                Thread.currentThread().getId(), key,
                StringUtils.stringifyException(e));
          }
        }

        if (parseResult == null) {
          byte[] signature = SignatureFactory.getSignature(conf)
              .calculate(content, new ParseStatus().getEmptyParse(conf));
          datum.setSignature(signature);
        }
      }

      /*
       * Store status code in content So we can read this value during parsing
       * (as a separate job) and decide to parse or not.
       */
      content.getMetadata().add(Nutch.FETCH_STATUS_KEY,
          Integer.toString(status));
    }

    try {
      context.write(key, new NutchWritable(datum));
      if (content != null && storingContent)
        context.write(key, new NutchWritable(content));
      if (parseResult != null) {
        for (Entry<Text, Parse> entry : parseResult) {
          Text url = entry.getKey();
          Parse parse = entry.getValue();
          ParseStatus parseStatus = parse.getData().getStatus();
          ParseData parseData = parse.getData();

          if (!parseStatus.isSuccess()) {
            LOG.warn("{} {} Error parsing: {}: {}", getName(),
                Thread.currentThread().getId(), key, parseStatus);
            parse = parseStatus.getEmptyParse(conf);
          }

          // Calculate page signature. For non-parsing fetchers this will
          // be done in ParseSegment
          byte[] signature = SignatureFactory.getSignature(conf)
              .calculate(content, parse);
          // Ensure segment name and score are in parseData metadata
          parseData.getContentMeta().set(Nutch.SEGMENT_NAME_KEY, segmentName);
          parseData.getContentMeta().set(Nutch.SIGNATURE_KEY,
              StringUtil.toHexString(signature));
          // Pass fetch time to content meta
          parseData.getContentMeta().set(Nutch.FETCH_TIME_KEY,
              Long.toString(datum.getFetchTime()));
          if (url.equals(key))
            datum.setSignature(signature);
          try {
            scfilters.passScoreAfterParsing(url, content, parse);
          } catch (Exception e) {
            if (LOG.isWarnEnabled()) {
              LOG.warn("{} {} Couldn't pass score, url {} ({})", getName(),
                  Thread.currentThread().getId(), key, e);
            }
          }

          String origin = null;

          // collect outlinks for subsequent db update
          Outlink[] links = parseData.getOutlinks();
          int outlinksToStore = Math.min(maxOutlinks, links.length);
          if (ignoreExternalLinks || ignoreInternalLinks) {
            URL originURL = new URL(url.toString());
            // based on domain?
            if ("bydomain".equalsIgnoreCase(ignoreExternalLinksMode)) {
              origin = URLUtil.getDomainName(originURL).toLowerCase();
            } 
            // use host 
            else {
              origin = originURL.getHost().toLowerCase();
            }
          }
          
          //used by fetchNode         
          if(fetchNode!=null){
            fetchNode.setOutlinks(links);
            fetchNode.setTitle(parseData.getTitle());
            FetchNodeDb.getInstance().put(fetchNode.getUrl().toString(), fetchNode);
          }
          int validCount = 0;

          // Process all outlinks, normalize, filter and deduplicate
          List<Outlink> outlinkList = new ArrayList<>(outlinksToStore);
          HashSet<String> outlinks = new HashSet<>(outlinksToStore);
          for (int i = 0; i < links.length && validCount < outlinksToStore; i++) {
            String toUrl = links[i].getToUrl();

            toUrl = ParseOutputFormat.filterNormalize(url.toString(), toUrl,
                origin, ignoreInternalLinks, ignoreExternalLinks,
                ignoreExternalLinksMode, urlFiltersForOutlinks,
                urlExemptionFilters, normalizersForOutlinks);
            if (toUrl == null) {
              continue;
            }

            validCount++;
            links[i].setUrl(toUrl);
            outlinkList.add(links[i]);
            outlinks.add(toUrl);
          }
          
          //Publish fetch report event 
          if(activatePublisher) {
            FetcherThreadEvent reportEvent = new FetcherThreadEvent(PublishEventType.REPORT, url.toString());
            reportEvent.addOutlinksToEventData(outlinkList);
            reportEvent.addEventData(Nutch.FETCH_EVENT_TITLE, parseData.getTitle());
            reportEvent.addEventData(Nutch.FETCH_EVENT_CONTENTTYPE, parseData.getContentMeta().get("content-type"));
            reportEvent.addEventData(Nutch.FETCH_EVENT_SCORE, datum.getScore());
            reportEvent.addEventData(Nutch.FETCH_EVENT_FETCHTIME, datum.getFetchTime());
            reportEvent.addEventData(Nutch.FETCH_EVENT_CONTENTLANG, parseData.getContentMeta().get("content-language"));
            publisher.publish(reportEvent, conf);
          }
          // Only process depth N outlinks
          if (maxOutlinkDepth > 0 && outlinkDepth < maxOutlinkDepth) {
            FetchItem ft = FetchItem.create(url, null, queueMode);
            FetchItemQueue queue = ((FetchItemQueues) fetchQueues).getFetchItemQueue(ft.queueID);
            queue.alreadyFetched.add(url.toString().hashCode());

            context.getCounter("FetcherOutlinks", "outlinks_detected").increment(
                outlinks.size());

            // Counter to limit num outlinks to follow per page
            int outlinkCounter = 0;

            String followUrl;

            // Walk over the outlinks and add as new FetchItem to the queues
            Iterator<String> iter = outlinks.iterator();
            while (iter.hasNext() && outlinkCounter < maxOutlinkDepthNumLinks) {
              followUrl = iter.next();

              // Check whether we'll follow external outlinks
              if (outlinksIgnoreExternal) {
                if (!URLUtil.getHost(url.toString()).equals(
                    URLUtil.getHost(followUrl))) {
                  continue;
                }
              }

              // Already followed?
              int urlHashCode = followUrl.hashCode();
              if (queue.alreadyFetched.contains(urlHashCode)) {
                continue;
              }
              queue.alreadyFetched.add(urlHashCode);
              
              // Create new FetchItem with depth incremented
              FetchItem fit = FetchItem.create(new Text(followUrl),
                  new CrawlDatum(CrawlDatum.STATUS_LINKED, interval),
                  queueMode, outlinkDepth + 1);
              
              context.getCounter("FetcherOutlinks", "outlinks_following").increment(1);    
              
              ((FetchItemQueues) fetchQueues).addFetchItem(fit);

              outlinkCounter++;
            }
          }

          // Overwrite the outlinks in ParseData with the normalized and
          // filtered set
          parseData.setOutlinks(outlinkList.toArray(new Outlink[outlinkList
              .size()]));

          context.write(url, new NutchWritable(new ParseImpl(new ParseText(
              parse.getText()), parseData, parse.isCanonical())));
        }
      }
    } catch (IOException e) {
      if (LOG.isErrorEnabled()) {
        LOG.error("fetcher caught:", e);
      }
    }

    // return parse status if it exits
    if (parseResult != null && !parseResult.isEmpty()) {
      Parse p = parseResult.get(content.getUrl());
      if (p != null) {
        context.getCounter("ParserStatus", ParseStatus.majorCodes[p
            .getData().getStatus().getMajorCode()]).increment(1);
        return p.getData().getStatus();
      }
    }
    return null;
  }
  
  private void outputRobotsTxt(List<Content> robotsTxtContent) throws InterruptedException {
    for (Content robotsTxt : robotsTxtContent) {
      LOG.debug("fetched and stored robots.txt {}",
          robotsTxt.getUrl());
      try {
        context.write(new Text(robotsTxt.getUrl()),
            new NutchWritable(robotsTxt));
      } catch (IOException e) {
        LOG.error("fetcher caught:", e);
      }
    }
  }

  private void updateStatus(int bytesInPage) throws IOException {
    pages.incrementAndGet();
    bytes.addAndGet(bytesInPage);
  }

  public synchronized void setHalted(boolean halted) {
    this.halted = halted;
  }

  public synchronized boolean isHalted() {
    return halted;
  }

  public String getReprUrl() {
    return reprUrl;
  }
  
  private void setReprUrl(String urlString) {
    this.reprUrl = urlString;
    
  }

}
