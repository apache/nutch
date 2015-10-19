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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.SignatureFactory;
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
  
  private static final Logger LOG = LoggerFactory.getLogger(FetcherThread.class);

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
  private boolean ignoreExternalLinks;

  // Used by fetcher.follow.outlinks.depth in parse
  private int maxOutlinksPerPage;
  private final int maxOutlinks;
  private final int interval;
  private int maxOutlinkDepth;
  private int maxOutlinkDepthNumLinks;
  private boolean outlinksIgnoreExternal;

  private int outlinksDepthDivisor;
  private boolean skipTruncated;

  private boolean halted = false;

  private AtomicInteger activeThreads;

  private Object fetchQueues;

  private QueueFeeder feeder;

  private Object spinWaiting;

  private AtomicLong lastRequestStart;

  private Reporter reporter;

  private AtomicInteger errors;

  private String segmentName;

  private boolean parsing;

  private OutputCollector<Text, NutchWritable> output;

  private boolean storingContent;

  private AtomicInteger pages;

  private AtomicLong bytes;
  
  //Used by the REST service
  private FetchNode fetchNode;
  private boolean reportToNutchServer;

  public FetcherThread(Configuration conf, AtomicInteger activeThreads, FetchItemQueues fetchQueues, 
      QueueFeeder feeder, AtomicInteger spinWaiting, AtomicLong lastRequestStart, Reporter reporter,
      AtomicInteger errors, String segmentName, boolean parsing, OutputCollector<Text, NutchWritable> output,
      boolean storingContent, AtomicInteger pages, AtomicLong bytes) {
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
    this.reporter = reporter;
    this.errors = errors;
    this.segmentName = segmentName;
    this.parsing = parsing;
    this.output = output;
    this.storingContent = storingContent;
    this.pages = pages;
    this.bytes = bytes;
    queueMode = conf.get("fetcher.queue.mode",
        FetchItemQueues.QUEUE_MODE_HOST);
    // check that the mode is known
    if (!queueMode.equals(FetchItemQueues.QUEUE_MODE_IP)
        && !queueMode.equals(FetchItemQueues.QUEUE_MODE_DOMAIN)
        && !queueMode.equals(FetchItemQueues.QUEUE_MODE_HOST)) {
      LOG.error("Unknown partition mode : " + queueMode
          + " - forcing to byHost");
      queueMode = FetchItemQueues.QUEUE_MODE_HOST;
    }
    LOG.info("Using queue mode : " + queueMode);
    this.maxRedirect = conf.getInt("http.redirect.max", 3);
    this.ignoreExternalLinks = conf.getBoolean("db.ignore.external.links",
        false);

    maxOutlinksPerPage = conf.getInt("db.max.outlinks.per.page", 100);
    maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE
        : maxOutlinksPerPage;
    interval = conf.getInt("db.fetch.interval.default", 2592000);
    ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);
    maxOutlinkDepth = conf.getInt("fetcher.follow.outlinks.depth", -1);
    outlinksIgnoreExternal = conf.getBoolean(
        "fetcher.follow.outlinks.ignore.external", false);
    maxOutlinkDepthNumLinks = conf.getInt(
        "fetcher.follow.outlinks.num.links", 4);
    outlinksDepthDivisor = conf.getInt(
        "fetcher.follow.outlinks.depth.divisor", 2);
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
          LOG.debug(getName() + " set to halted");
          fit = null;
          return;
        }

        fit = ((FetchItemQueues) fetchQueues).getFetchItem();
        if (fit == null) {
          if (feeder.isAlive() || ((FetchItemQueues) fetchQueues).getTotalSize() > 0) {
            LOG.debug(getName() + " spin-waiting ...");
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
            LOG.info("Thread " + getName() + " has no more work available");
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
          do {
            if (LOG.isInfoEnabled()) {
              LOG.info("fetching " + fit.url + " (queue crawl delay="
                  + ((FetchItemQueues) fetchQueues).getFetchItemQueue(fit.queueID).crawlDelay
                  + "ms)");
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("redirectCount=" + redirectCount);
            }
            redirecting = false;
            Protocol protocol = this.protocolFactory.getProtocol(fit.url
                .toString());
            BaseRobotRules rules = protocol.getRobotRules(fit.url, fit.datum);
            if (!rules.isAllowed(fit.u.toString())) {
              // unblock
              ((FetchItemQueues) fetchQueues).finishFetchItem(fit, true);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Denied by robots.txt: " + fit.url);
              }
              output(fit.url, fit.datum, null,
                  ProtocolStatus.STATUS_ROBOTS_DENIED,
                  CrawlDatum.STATUS_FETCH_GONE);
              reporter.incrCounter("FetcherStatus", "robots_denied", 1);
              continue;
            }
            if (rules.getCrawlDelay() > 0) {
              if (rules.getCrawlDelay() > maxCrawlDelay && maxCrawlDelay >= 0) {
                // unblock
                ((FetchItemQueues) fetchQueues).finishFetchItem(fit, true);
                LOG.debug("Crawl-Delay for " + fit.url + " too long ("
                    + rules.getCrawlDelay() + "), skipping");
                output(fit.url, fit.datum, null,
                    ProtocolStatus.STATUS_ROBOTS_DENIED,
                    CrawlDatum.STATUS_FETCH_GONE);
                reporter.incrCounter("FetcherStatus",
                    "robots_denied_maxcrawldelay", 1);
                continue;
              } else {
                FetchItemQueue fiq = ((FetchItemQueues) fetchQueues)
                    .getFetchItemQueue(fit.queueID);
                fiq.crawlDelay = rules.getCrawlDelay();
                if (LOG.isDebugEnabled()) {
                  LOG.info("Crawl delay for queue: " + fit.queueID
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

            String urlString = fit.url.toString();
            
            // used for FetchNode
            if (fetchNode != null) {
              fetchNode.setStatus(status.getCode());
              fetchNode.setFetchTime(System.currentTimeMillis());
              fetchNode.setUrl(fit.url);
            }

            reporter.incrCounter("FetcherStatus", status.getName(), 1);

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
                Text redirUrl = handleRedirect(fit.url, fit.datum, urlString,
                    newUrl, refreshTime < Fetcher.PERM_REFRESH_TIME,
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
              Text redirUrl = handleRedirect(fit.url, fit.datum, urlString,
                  newUrl, temp, Fetcher.PROTOCOL_REDIR);
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
                reporter.incrCounter("FetcherStatus",
                    "AboveExceptionThresholdInQueue", killedURLs);
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
                LOG.warn("Unknown ProtocolStatus: " + status.getCode());
              }
              output(fit.url, fit.datum, null, status,
                  CrawlDatum.STATUS_FETCH_RETRY);
            }

            if (redirecting && redirectCount > maxRedirect) {
              ((FetchItemQueues) fetchQueues).finishFetchItem(fit);
              if (LOG.isInfoEnabled()) {
                LOG.info(" - redirect count exceeded " + fit.url);
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
        LOG.error("fetcher caught:" + e.toString());
      }
    } finally {
      if (fit != null)
        ((FetchItemQueues) fetchQueues).finishFetchItem(fit);
      activeThreads.decrementAndGet(); // count threads
      LOG.info("-finishing thread " + getName() + ", activeThreads="
          + activeThreads);
    }
  }

  private Text handleRedirect(Text url, CrawlDatum datum, String urlString,
      String newUrl, boolean temp, String redirType)
      throws MalformedURLException, URLFilterException {
    newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
    newUrl = urlFilters.filter(newUrl);

    if (ignoreExternalLinks) {
      try {
        String origHost = new URL(urlString).getHost().toLowerCase();
        String newHost = new URL(newUrl).getHost().toLowerCase();
        if (!origHost.equals(newHost)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(" - ignoring redirect " + redirType + " from "
                + urlString + " to " + newUrl
                + " because external links are ignored");
          }
          return null;
        }
      } catch (MalformedURLException e) {
      }
    }

    if (newUrl != null && !newUrl.equals(urlString)) {
      reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temp);
      url = new Text(newUrl);
      if (maxRedirect > 0) {
        redirecting = true;
        redirectCount++;
        if (LOG.isDebugEnabled()) {
          LOG.debug(" - " + redirType + " redirect to " + url
              + " (fetching now)");
        }
        return url;
      } else {
        CrawlDatum newDatum = new CrawlDatum(CrawlDatum.STATUS_LINKED,
            datum.getFetchInterval(), datum.getScore());
        // transfer existing metadata
        newDatum.getMetaData().putAll(datum.getMetaData());
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
        if (LOG.isDebugEnabled()) {
          LOG.debug(" - " + redirType + " redirect to " + url
              + " (fetching later)");
        }
        return null;
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(" - " + redirType + " redirect skipped: "
            + (newUrl != null ? "to same url" : "filtered"));
      }
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
      reporter.incrCounter("FetcherStatus", "FetchItem.notCreated.redirect",
          1);
    }
    return fit;
  }

  private void logError(Text url, String message) {
    if (LOG.isInfoEnabled()) {
      LOG.info("fetch of " + url + " failed with: " + message);
    }
    errors.incrementAndGet();
  }

  private ParseStatus output(Text key, CrawlDatum datum, Content content,
      ProtocolStatus pstatus, int status) {

    return output(key, datum, content, pstatus, status, 0);
  }

  private ParseStatus output(Text key, CrawlDatum datum, Content content,
      ProtocolStatus pstatus, int status, int outlinkDepth) {

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
          LOG.warn("Couldn't pass score, url " + key + " (" + e + ")");
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
            LOG.warn("Error parsing: " + key + ": "
                + StringUtils.stringifyException(e));
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
      output.collect(key, new NutchWritable(datum));
      if (content != null && storingContent)
        output.collect(key, new NutchWritable(content));
      if (parseResult != null) {
        for (Entry<Text, Parse> entry : parseResult) {
          Text url = entry.getKey();
          Parse parse = entry.getValue();
          ParseStatus parseStatus = parse.getData().getStatus();
          ParseData parseData = parse.getData();

          if (!parseStatus.isSuccess()) {
            LOG.warn("Error parsing: " + key + ": " + parseStatus);
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
              LOG.warn("Couldn't pass score, url " + key + " (" + e + ")");
            }
          }

          String fromHost;

          // collect outlinks for subsequent db update
          Outlink[] links = parseData.getOutlinks();
          int outlinksToStore = Math.min(maxOutlinks, links.length);
          if (ignoreExternalLinks) {
            try {
              fromHost = new URL(url.toString()).getHost().toLowerCase();
            } catch (MalformedURLException e) {
              fromHost = null;
            }
          } else {
            fromHost = null;
          }
          
          //used by fetchNode         
          if(fetchNode!=null){
            fetchNode.setOutlinks(links);
            fetchNode.setTitle(parseData.getTitle());
            FetchNodeDb.getInstance().put(fetchNode.getUrl().toString(), fetchNode);
          }
          int validCount = 0;

          // Process all outlinks, normalize, filter and deduplicate
          List<Outlink> outlinkList = new ArrayList<Outlink>(outlinksToStore);
          HashSet<String> outlinks = new HashSet<String>(outlinksToStore);
          for (int i = 0; i < links.length && validCount < outlinksToStore; i++) {
            String toUrl = links[i].getToUrl();

            toUrl = ParseOutputFormat.filterNormalize(url.toString(), toUrl,
                fromHost, ignoreExternalLinks, urlFilters, urlExemptionFilters, normalizers);
            if (toUrl == null) {
              continue;
            }

            validCount++;
            links[i].setUrl(toUrl);
            outlinkList.add(links[i]);
            outlinks.add(toUrl);
          }

          // Only process depth N outlinks
          if (maxOutlinkDepth > 0 && outlinkDepth < maxOutlinkDepth) {
            reporter.incrCounter("FetcherOutlinks", "outlinks_detected",
                outlinks.size());

            // Counter to limit num outlinks to follow per page
            int outlinkCounter = 0;

            // Calculate variable number of outlinks by depth using the
            // divisor (outlinks = Math.floor(divisor / depth * num.links))
            int maxOutlinksByDepth = (int) Math.floor(outlinksDepthDivisor
                / (outlinkDepth + 1) * maxOutlinkDepthNumLinks);

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

              reporter
                  .incrCounter("FetcherOutlinks", "outlinks_following", 1);

              // Create new FetchItem with depth incremented
              FetchItem fit = FetchItem.create(new Text(followUrl),
                  new CrawlDatum(CrawlDatum.STATUS_LINKED, interval),
                  queueMode, outlinkDepth + 1);
              ((FetchItemQueues) fetchQueues).addFetchItem(fit);

              outlinkCounter++;
            }
          }

          // Overwrite the outlinks in ParseData with the normalized and
          // filtered set
          parseData.setOutlinks(outlinkList.toArray(new Outlink[outlinkList
              .size()]));

          output.collect(url, new NutchWritable(new ParseImpl(new ParseText(
              parse.getText()), parseData, parse.isCanonical())));
        }
      }
    } catch (IOException e) {
      if (LOG.isErrorEnabled()) {
        LOG.error("fetcher caught:" + e.toString());
      }
    }

    // return parse status if it exits
    if (parseResult != null && !parseResult.isEmpty()) {
      Parse p = parseResult.get(content.getUrl());
      if (p != null) {
        reporter.incrCounter("ParserStatus", ParseStatus.majorCodes[p
            .getData().getStatus().getMajorCode()], 1);
        return p.getData().getStatus();
      }
    }
    return null;
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
