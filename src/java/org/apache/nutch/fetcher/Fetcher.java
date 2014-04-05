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
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// Slf4j Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.*;

import crawlercommons.robots.BaseRobotRules;

/**
 * A queue-based fetcher.
 *
 * <p>This fetcher uses a well-known model of one producer (a QueueFeeder)
 * and many consumers (FetcherThread-s).
 *
 * <p>QueueFeeder reads input fetchlists and
 * populates a set of FetchItemQueue-s, which hold FetchItem-s that
 * describe the items to be fetched. There are as many queues as there are unique
 * hosts, but at any given time the total number of fetch items in all queues
 * is less than a fixed number (currently set to a multiple of the number of
 * threads).
 *
 * <p>As items are consumed from the queues, the QueueFeeder continues to add new
 * input items, so that their total count stays fixed (FetcherThread-s may also
 * add new items to the queues e.g. as a results of redirection) - until all
 * input items are exhausted, at which point the number of items in the queues
 * begins to decrease. When this number reaches 0 fetcher will finish.
 *
 * <p>This fetcher implementation handles per-host blocking itself, instead
 * of delegating this work to protocol-specific plugins.
 * Each per-host queue handles its own "politeness" settings, such as the
 * maximum number of concurrent requests and crawl delay between consecutive
 * requests - and also a list of requests in progress, and the time the last
 * request was finished. As FetcherThread-s ask for new items to be fetched,
 * queues may return eligible items or null if for "politeness" reasons this
 * host's queue is not yet ready.
 *
 * <p>If there are still unfetched items in the queues, but none of the items
 * are ready, FetcherThread-s will spin-wait until either some items become
 * available, or a timeout is reached (at which point the Fetcher will abort,
 * assuming the task is hung).
 *
 * @author Andrzej Bialecki
 */
public class Fetcher extends Configured implements Tool,
    MapRunnable<Text, CrawlDatum, Text, NutchWritable> {

  public static final int PERM_REFRESH_TIME = 5;

  public static final String CONTENT_REDIR = "content";

  public static final String PROTOCOL_REDIR = "protocol";

  public static final Logger LOG = LoggerFactory.getLogger(Fetcher.class);

  public static class InputFormat extends SequenceFileInputFormat<Text, CrawlDatum> {
    /** Don't split inputs, to keep things polite. */
    public InputSplit[] getSplits(JobConf job, int nSplits)
      throws IOException {
      FileStatus[] files = listStatus(job);
      FileSplit[] splits = new FileSplit[files.length];
      for (int i = 0; i < files.length; i++) {
        FileStatus cur = files[i];
        splits[i] = new FileSplit(cur.getPath(), 0,
            cur.getLen(), (String[])null);
      }
      return splits;
    }
  }

  private OutputCollector<Text, NutchWritable> output;
  private Reporter reporter;

  private String segmentName;
  private AtomicInteger activeThreads = new AtomicInteger(0);
  private AtomicInteger spinWaiting = new AtomicInteger(0);

  private long start = System.currentTimeMillis(); // start time of fetcher run
  private AtomicLong lastRequestStart = new AtomicLong(start);

  private AtomicLong bytes = new AtomicLong(0);        // total bytes fetched
  private AtomicInteger pages = new AtomicInteger(0);  // total pages fetched
  private AtomicInteger errors = new AtomicInteger(0); // total pages errored

  private boolean storingContent;
  private boolean parsing;
  FetchItemQueues fetchQueues;
  QueueFeeder feeder;

  /**
   * This class described the item to be fetched.
   */
  private static class FetchItem {
    int outlinkDepth = 0;
    String queueID;
    Text url;
    URL u;
    CrawlDatum datum;

    public FetchItem(Text url, URL u, CrawlDatum datum, String queueID) {
      this(url, u, datum, queueID, 0);
    }

    public FetchItem(Text url, URL u, CrawlDatum datum, String queueID, int outlinkDepth) {
      this.url = url;
      this.u = u;
      this.datum = datum;
      this.queueID = queueID;
      this.outlinkDepth = outlinkDepth;
    }

    /** Create an item. Queue id will be created based on <code>queueMode</code>
     * argument, either as a protocol + hostname pair, protocol + IP
     * address pair or protocol+domain pair.
     */
    public static FetchItem create(Text url, CrawlDatum datum,  String queueMode) {
      return create(url, datum, queueMode, 0);
    }

    public static FetchItem create(Text url, CrawlDatum datum,  String queueMode, int outlinkDepth) {
      String queueID;
      URL u = null;
      try {
        u = new URL(url.toString());
      } catch (Exception e) {
        LOG.warn("Cannot parse url: " + url, e);
        return null;
      }
      final String proto = u.getProtocol().toLowerCase();
      String key;
      if (FetchItemQueues.QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
        try {
          final InetAddress addr = InetAddress.getByName(u.getHost());
          key = addr.getHostAddress();
        } catch (final UnknownHostException e) {
          // unable to resolve it, so don't fall back to host name
          LOG.warn("Unable to resolve: " + u.getHost() + ", skipping.");
          return null;
        }
      }
      else if (FetchItemQueues.QUEUE_MODE_DOMAIN.equalsIgnoreCase(queueMode)){
        key = URLUtil.getDomainName(u);
        if (key == null) {
          LOG.warn("Unknown domain for url: " + url + ", using URL string as key");
          key=u.toExternalForm();
        }
      }
      else {
        key = u.getHost();
        if (key == null) {
          LOG.warn("Unknown host for url: " + url + ", using URL string as key");
          key=u.toExternalForm();
        }
      }
      queueID = proto + "://" + key.toLowerCase();
      return new FetchItem(url, u, datum, queueID, outlinkDepth);
    }

    public CrawlDatum getDatum() {
      return datum;
    }

    public String getQueueID() {
      return queueID;
    }

    public Text getUrl() {
      return url;
    }

    public URL getURL2() {
      return u;
    }
  }

  /**
   * This class handles FetchItems which come from the same host ID (be it
   * a proto/hostname or proto/IP pair). It also keeps track of requests in
   * progress and elapsed time between requests.
   */
  private static class FetchItemQueue {
    List<FetchItem> queue = Collections.synchronizedList(new LinkedList<FetchItem>());
    AtomicInteger  inProgress = new AtomicInteger();
    AtomicLong nextFetchTime = new AtomicLong();
    AtomicInteger exceptionCounter = new AtomicInteger();
    long crawlDelay;
    long minCrawlDelay;
    int maxThreads;
    Configuration conf;

    public FetchItemQueue(Configuration conf, int maxThreads, long crawlDelay, long minCrawlDelay) {
      this.conf = conf;
      this.maxThreads = maxThreads;
      this.crawlDelay = crawlDelay;
      this.minCrawlDelay = minCrawlDelay;
      // ready to start
      setEndTime(System.currentTimeMillis() - crawlDelay);
    }

    public synchronized int emptyQueue() {
      int presize = queue.size();
      queue.clear();
      return presize;
    }

    public int getQueueSize() {
      return queue.size();
    }

    public int getInProgressSize() {
      return inProgress.get();
    }

    public int incrementExceptionCounter() {
      return exceptionCounter.incrementAndGet();
    }

    public void finishFetchItem(FetchItem it, boolean asap) {
      if (it != null) {
        inProgress.decrementAndGet();
        setEndTime(System.currentTimeMillis(), asap);
      }
    }

    public void addFetchItem(FetchItem it) {
      if (it == null) return;
      queue.add(it);
    }

    public void addInProgressFetchItem(FetchItem it) {
      if (it == null) return;
      inProgress.incrementAndGet();
    }

    public FetchItem getFetchItem() {
      if (inProgress.get() >= maxThreads) return null;
      long now = System.currentTimeMillis();
      if (nextFetchTime.get() > now) return null;
      FetchItem it = null;
      if (queue.size() == 0) return null;
      try {
        it = queue.remove(0);
        inProgress.incrementAndGet();
      } catch (Exception e) {
        LOG.error("Cannot remove FetchItem from queue or cannot add it to inProgress queue", e);
      }
      return it;
    }

    public synchronized void dump() {
      LOG.info("  maxThreads    = " + maxThreads);
      LOG.info("  inProgress    = " + inProgress.get());
      LOG.info("  crawlDelay    = " + crawlDelay);
      LOG.info("  minCrawlDelay = " + minCrawlDelay);
      LOG.info("  nextFetchTime = " + nextFetchTime.get());
      LOG.info("  now           = " + System.currentTimeMillis());
      for (int i = 0; i < queue.size(); i++) {
        FetchItem it = queue.get(i);
        LOG.info("  " + i + ". " + it.url);
      }
    }

    private void setEndTime(long endTime) {
      setEndTime(endTime, false);
    }

    private void setEndTime(long endTime, boolean asap) {
      if (!asap)
        nextFetchTime.set(endTime + (maxThreads > 1 ? minCrawlDelay : crawlDelay));
      else
        nextFetchTime.set(endTime);
    }
  }

  /**
   * Convenience class - a collection of queues that keeps track of the total
   * number of items, and provides items eligible for fetching from any queue.
   */
  private static class FetchItemQueues {
    public static final String DEFAULT_ID = "default";
    Map<String, FetchItemQueue> queues = new HashMap<String, FetchItemQueue>();
    AtomicInteger totalSize = new AtomicInteger(0);
    int maxThreads;
    long crawlDelay;
    long minCrawlDelay;
    long timelimit = -1;
    int maxExceptionsPerQueue = -1;
    Configuration conf;

    public static final String QUEUE_MODE_HOST = "byHost";
    public static final String QUEUE_MODE_DOMAIN = "byDomain";
    public static final String QUEUE_MODE_IP = "byIP";

    String queueMode;

    public FetchItemQueues(Configuration conf) {
      this.conf = conf;
      this.maxThreads = conf.getInt("fetcher.threads.per.queue", 1);
      queueMode = conf.get("fetcher.queue.mode", QUEUE_MODE_HOST);
      // check that the mode is known
      if (!queueMode.equals(QUEUE_MODE_IP) && !queueMode.equals(QUEUE_MODE_DOMAIN)
          && !queueMode.equals(QUEUE_MODE_HOST)) {
        LOG.error("Unknown partition mode : " + queueMode + " - forcing to byHost");
        queueMode = QUEUE_MODE_HOST;
      }
      LOG.info("Using queue mode : "+queueMode);

      this.crawlDelay = (long) (conf.getFloat("fetcher.server.delay", 1.0f) * 1000);
      this.minCrawlDelay = (long) (conf.getFloat("fetcher.server.min.delay", 0.0f) * 1000);
      this.timelimit = conf.getLong("fetcher.timelimit", -1);
      this.maxExceptionsPerQueue = conf.getInt("fetcher.max.exceptions.per.queue", -1);
    }

    public int getTotalSize() {
      return totalSize.get();
    }

    public int getQueueCount() {
      return queues.size();
    }

    public void addFetchItem(Text url, CrawlDatum datum) {
      FetchItem it = FetchItem.create(url, datum, queueMode);
      if (it != null) addFetchItem(it);
    }

    public synchronized void addFetchItem(FetchItem it) {
      FetchItemQueue fiq = getFetchItemQueue(it.queueID);
      fiq.addFetchItem(it);
      totalSize.incrementAndGet();
    }

    public void finishFetchItem(FetchItem it) {
      finishFetchItem(it, false);
    }

    public void finishFetchItem(FetchItem it, boolean asap) {
      FetchItemQueue fiq = queues.get(it.queueID);
      if (fiq == null) {
        LOG.warn("Attempting to finish item from unknown queue: " + it);
        return;
      }
      fiq.finishFetchItem(it, asap);
    }

    public synchronized FetchItemQueue getFetchItemQueue(String id) {
      FetchItemQueue fiq = queues.get(id);
      if (fiq == null) {
        // initialize queue
        fiq = new FetchItemQueue(conf, maxThreads, crawlDelay, minCrawlDelay);
        queues.put(id, fiq);
      }
      return fiq;
    }

    public synchronized FetchItem getFetchItem() {
      Iterator<Map.Entry<String, FetchItemQueue>> it =
        queues.entrySet().iterator();
      while (it.hasNext()) {
        FetchItemQueue fiq = it.next().getValue();
        // reap empty queues
        if (fiq.getQueueSize() == 0 && fiq.getInProgressSize() == 0) {
          it.remove();
          continue;
        }
        FetchItem fit = fiq.getFetchItem();
        if (fit != null) {
          totalSize.decrementAndGet();
          return fit;
        }
      }
      return null;
    }

    // called only once the feeder has stopped
    public synchronized int checkTimelimit() {
      int count = 0;

      if (System.currentTimeMillis() >= timelimit && timelimit != -1) {
        // emptying the queues
        count = emptyQueues();

        // there might also be a case where totalsize !=0 but number of queues
        // == 0
        // in which case we simply force it to 0 to avoid blocking
        if (totalSize.get() != 0 && queues.size() == 0) totalSize.set(0);
      }
      return count;
    }

    // empties the queues (used by timebomb and throughput threshold)
    public synchronized int emptyQueues() {
      int count = 0;

      for (String id : queues.keySet()) {
        FetchItemQueue fiq = queues.get(id);
        if (fiq.getQueueSize() == 0) continue;
        LOG.info("* queue: " + id + " >> dropping! ");
        int deleted = fiq.emptyQueue();
        for (int i = 0; i < deleted; i++) {
          totalSize.decrementAndGet();
        }
        count += deleted;
      }

      return count;
    }

    /**
     * Increment the exception counter of a queue in case of an exception e.g.
     * timeout; when higher than a given threshold simply empty the queue.
     *
     * @param queueid
     * @return number of purged items
     */
    public synchronized int checkExceptionThreshold(String queueid) {
      FetchItemQueue fiq = queues.get(queueid);
      if (fiq == null) {
        return 0;
      }
      if (fiq.getQueueSize() == 0) {
        return 0;
      }
      int excCount = fiq.incrementExceptionCounter();
      if (maxExceptionsPerQueue!= -1 && excCount >= maxExceptionsPerQueue) {
        // too many exceptions for items in this queue - purge it
        int deleted = fiq.emptyQueue();
        LOG.info("* queue: " + queueid + " >> removed " + deleted
            + " URLs from queue because " + excCount + " exceptions occurred");
        for (int i = 0; i < deleted; i++) {
          totalSize.decrementAndGet();
        }
        return deleted;
      }
      return 0;
    }


    public synchronized void dump() {
      for (String id : queues.keySet()) {
        FetchItemQueue fiq = queues.get(id);
        if (fiq.getQueueSize() == 0) continue;
        LOG.info("* queue: " + id);
        fiq.dump();
      }
    }
  }

  /**
   * This class feeds the queues with input items, and re-fills them as
   * items are consumed by FetcherThread-s.
   */
  private static class QueueFeeder extends Thread {
    private RecordReader<Text, CrawlDatum> reader;
    private FetchItemQueues queues;
    private int size;
    private long timelimit = -1;

    public QueueFeeder(RecordReader<Text, CrawlDatum> reader,
        FetchItemQueues queues, int size) {
      this.reader = reader;
      this.queues = queues;
      this.size = size;
      this.setDaemon(true);
      this.setName("QueueFeeder");
    }

    public void setTimeLimit(long tl) {
      timelimit = tl;
    }

    public void run() {
      boolean hasMore = true;
      int cnt = 0;
      int timelimitcount = 0;
      while (hasMore) {
        if (System.currentTimeMillis() >= timelimit && timelimit != -1) {
          // enough .. lets' simply
          // read all the entries from the input without processing them
          try {
            Text url = new Text();
            CrawlDatum datum = new CrawlDatum();
            hasMore = reader.next(url, datum);
            timelimitcount++;
          } catch (IOException e) {
            LOG.error("QueueFeeder error reading input, record " + cnt, e);
            return;
          }
          continue;
        }
        int feed = size - queues.getTotalSize();
        if (feed <= 0) {
          // queues are full - spin-wait until they have some free space
          try {
            Thread.sleep(1000);
          } catch (Exception e) {};
          continue;
        } else {
          LOG.debug("-feeding " + feed + " input urls ...");
          while (feed > 0 && hasMore) {
            try {
              Text url = new Text();
              CrawlDatum datum = new CrawlDatum();
              hasMore = reader.next(url, datum);
              if (hasMore) {
                queues.addFetchItem(url, datum);
                cnt++;
                feed--;
              }
            } catch (IOException e) {
              LOG.error("QueueFeeder error reading input, record " + cnt, e);
              return;
            }
          }
        }
      }
      LOG.info("QueueFeeder finished: total " + cnt + " records + hit by time limit :"
          + timelimitcount);
    }
  }

  /**
   * This class picks items from queues and fetches the pages.
   */
  private class FetcherThread extends Thread {
    private Configuration conf;
    private URLFilters urlFilters;
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

    public FetcherThread(Configuration conf) {
      this.setDaemon(true);                       // don't hang JVM on exit
      this.setName("FetcherThread");              // use an informative name
      this.conf = conf;
      this.urlFilters = new URLFilters(conf);
      this.scfilters = new ScoringFilters(conf);
      this.parseUtil = new ParseUtil(conf);
      this.skipTruncated = conf.getBoolean(ParseSegment.SKIP_TRUNCATED, true);
      this.protocolFactory = new ProtocolFactory(conf);
      this.normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_FETCHER);
      this.maxCrawlDelay = conf.getInt("fetcher.max.crawl.delay", 30) * 1000;
      queueMode = conf.get("fetcher.queue.mode", FetchItemQueues.QUEUE_MODE_HOST);
      // check that the mode is known
      if (!queueMode.equals(FetchItemQueues.QUEUE_MODE_IP) && !queueMode.equals(FetchItemQueues.QUEUE_MODE_DOMAIN)
          && !queueMode.equals(FetchItemQueues.QUEUE_MODE_HOST)) {
        LOG.error("Unknown partition mode : " + queueMode + " - forcing to byHost");
        queueMode = FetchItemQueues.QUEUE_MODE_HOST;
      }
      LOG.info("Using queue mode : "+queueMode);
      this.maxRedirect = conf.getInt("http.redirect.max", 3);
      this.ignoreExternalLinks =
        conf.getBoolean("db.ignore.external.links", false);

      maxOutlinksPerPage = conf.getInt("db.max.outlinks.per.page", 100);
      maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE : maxOutlinksPerPage;
      interval = conf.getInt("db.fetch.interval.default", 2592000);
      ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);
      maxOutlinkDepth = conf.getInt("fetcher.follow.outlinks.depth", -1);
      outlinksIgnoreExternal = conf.getBoolean("fetcher.follow.outlinks.ignore.external", false);
      maxOutlinkDepthNumLinks = conf.getInt("fetcher.follow.outlinks.num.links", 4);
      outlinksDepthDivisor = conf.getInt("fetcher.follow.outlinks.depth.divisor", 2);
    }

    @SuppressWarnings("fallthrough")
    public void run() {
      activeThreads.incrementAndGet(); // count threads

      FetchItem fit = null;
      try {

        while (true) {
          fit = fetchQueues.getFetchItem();
          if (fit == null) {
            if (feeder.isAlive() || fetchQueues.getTotalSize() > 0) {
              LOG.debug(getName() + " spin-waiting ...");
              // spin-wait.
              spinWaiting.incrementAndGet();
              try {
                Thread.sleep(500);
              } catch (Exception e) {}
                spinWaiting.decrementAndGet();
              continue;
            } else {
              // all done, finish this thread
              return;
            }
          }
          lastRequestStart.set(System.currentTimeMillis());
          Text reprUrlWritable =
            (Text) fit.datum.getMetaData().get(Nutch.WRITABLE_REPR_URL_KEY);
          if (reprUrlWritable == null) {
            reprUrl = fit.url.toString();
          } else {
            reprUrl = reprUrlWritable.toString();
          }
          try {
            // fetch the page
            redirecting = false;
            redirectCount = 0;
            do {
              if (LOG.isInfoEnabled()) {
                LOG.info("fetching " + fit.url + " (queue crawl delay=" + 
                         fetchQueues.getFetchItemQueue(fit.queueID).crawlDelay + "ms)"); 
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("redirectCount=" + redirectCount);
              }
              redirecting = false;
              Protocol protocol = this.protocolFactory.getProtocol(fit.url.toString());
              BaseRobotRules rules = protocol.getRobotRules(fit.url, fit.datum);
              if (!rules.isAllowed(fit.u.toString())) {
                // unblock
                fetchQueues.finishFetchItem(fit, true);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Denied by robots.txt: " + fit.url);
                }
                output(fit.url, fit.datum, null, ProtocolStatus.STATUS_ROBOTS_DENIED, CrawlDatum.STATUS_FETCH_GONE);
                reporter.incrCounter("FetcherStatus", "robots_denied", 1);
                continue;
              }
              if (rules.getCrawlDelay() > 0) {
                if (rules.getCrawlDelay() > maxCrawlDelay && maxCrawlDelay >= 0) {
                  // unblock
                  fetchQueues.finishFetchItem(fit, true);
                  LOG.debug("Crawl-Delay for " + fit.url + " too long (" + rules.getCrawlDelay() + "), skipping");
                  output(fit.url, fit.datum, null, ProtocolStatus.STATUS_ROBOTS_DENIED, CrawlDatum.STATUS_FETCH_GONE);
                  reporter.incrCounter("FetcherStatus", "robots_denied_maxcrawldelay", 1);
                  continue;
                } else {
                  FetchItemQueue fiq = fetchQueues.getFetchItemQueue(fit.queueID);
                  fiq.crawlDelay = rules.getCrawlDelay();
                  if (LOG.isDebugEnabled()) {
                    LOG.info("Crawl delay for queue: " + fit.queueID + " is set to " + fiq.crawlDelay + " as per robots.txt. url: " + fit.url);
                  }
                }
              }
              ProtocolOutput output = protocol.getProtocolOutput(fit.url, fit.datum);
              ProtocolStatus status = output.getStatus();
              Content content = output.getContent();
              ParseStatus pstatus = null;
              // unblock queue
              fetchQueues.finishFetchItem(fit);

              String urlString = fit.url.toString();

              reporter.incrCounter("FetcherStatus", status.getName(), 1);

              switch(status.getCode()) {

              case ProtocolStatus.WOULDBLOCK:
                // retry ?
                fetchQueues.addFetchItem(fit);
                break;

              case ProtocolStatus.SUCCESS:        // got a page
                pstatus = output(fit.url, fit.datum, content, status, CrawlDatum.STATUS_FETCH_SUCCESS, fit.outlinkDepth);
                updateStatus(content.getContent().length);
                if (pstatus != null && pstatus.isSuccess() &&
                        pstatus.getMinorCode() == ParseStatus.SUCCESS_REDIRECT) {
                  String newUrl = pstatus.getMessage();
                  int refreshTime = Integer.valueOf(pstatus.getArgs()[1]);
                  Text redirUrl =
                    handleRedirect(fit.url, fit.datum,
                                   urlString, newUrl,
                                   refreshTime < Fetcher.PERM_REFRESH_TIME,
                                   Fetcher.CONTENT_REDIR);
                  if (redirUrl != null) {
                    queueRedirect(redirUrl, fit);
                  }
                }
                break;

              case ProtocolStatus.MOVED:         // redirect
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
                Text redirUrl =
                  handleRedirect(fit.url, fit.datum,
                                 urlString, newUrl, temp,
                                 Fetcher.PROTOCOL_REDIR);
                if (redirUrl != null) {
                  queueRedirect(redirUrl, fit);
                } else {
                  // stop redirecting
                  redirecting = false;
                }
                break;

              case ProtocolStatus.EXCEPTION:
                logError(fit.url, status.getMessage());
                int killedURLs = fetchQueues.checkExceptionThreshold(fit.getQueueID());
                if (killedURLs!=0)
                   reporter.incrCounter("FetcherStatus", "AboveExceptionThresholdInQueue", killedURLs);
                /* FALLTHROUGH */
              case ProtocolStatus.RETRY:          // retry
              case ProtocolStatus.BLOCKED:
                output(fit.url, fit.datum, null, status, CrawlDatum.STATUS_FETCH_RETRY);
                break;

              case ProtocolStatus.GONE:           // gone
              case ProtocolStatus.NOTFOUND:
              case ProtocolStatus.ACCESS_DENIED:
              case ProtocolStatus.ROBOTS_DENIED:
                output(fit.url, fit.datum, null, status, CrawlDatum.STATUS_FETCH_GONE);
                break;

              case ProtocolStatus.NOTMODIFIED:
                output(fit.url, fit.datum, null, status, CrawlDatum.STATUS_FETCH_NOTMODIFIED);
                break;

              default:
                if (LOG.isWarnEnabled()) {
                  LOG.warn("Unknown ProtocolStatus: " + status.getCode());
                }
                output(fit.url, fit.datum, null, status, CrawlDatum.STATUS_FETCH_RETRY);
              }

              if (redirecting && redirectCount > maxRedirect) {
                fetchQueues.finishFetchItem(fit);
                if (LOG.isInfoEnabled()) {
                  LOG.info(" - redirect count exceeded " + fit.url);
                }
                output(fit.url, fit.datum, null, ProtocolStatus.STATUS_REDIR_EXCEEDED, CrawlDatum.STATUS_FETCH_GONE);
              }

            } while (redirecting && (redirectCount <= maxRedirect));

          } catch (Throwable t) {                 // unexpected exception
            // unblock
            fetchQueues.finishFetchItem(fit);
            logError(fit.url, StringUtils.stringifyException(t));
            output(fit.url, fit.datum, null, ProtocolStatus.STATUS_FAILED, CrawlDatum.STATUS_FETCH_RETRY);
          }
        }

      } catch (Throwable e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("fetcher caught:"+e.toString());
        }
      } finally {
        if (fit != null) fetchQueues.finishFetchItem(fit);
        activeThreads.decrementAndGet(); // count threads
        LOG.info("-finishing thread " + getName() + ", activeThreads=" + activeThreads);
      }
    }

    private Text handleRedirect(Text url, CrawlDatum datum,
                                String urlString, String newUrl,
                                boolean temp, String redirType)
    throws MalformedURLException, URLFilterException {
      newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
      newUrl = urlFilters.filter(newUrl);

      if (ignoreExternalLinks) {
        try {
          String origHost = new URL(urlString).getHost().toLowerCase();
          String newHost = new URL(newUrl).getHost().toLowerCase();
          if (!origHost.equals(newHost)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(" - ignoring redirect " + redirType + " from " +
                          urlString + " to " + newUrl +
                          " because external links are ignored");
            }
            return null;
          }
        } catch (MalformedURLException e) { }
      }

      if (newUrl != null && !newUrl.equals(urlString)) {
        reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temp);
        url = new Text(newUrl);
        if (maxRedirect > 0) {
          redirecting = true;
          redirectCount++;
          if (LOG.isDebugEnabled()) {
            LOG.debug(" - " + redirType + " redirect to " +
                url + " (fetching now)");
          }
          return url;
        } else {
          CrawlDatum newDatum = new CrawlDatum(CrawlDatum.STATUS_LINKED,
              datum.getFetchInterval(),datum.getScore());
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
            LOG.debug(" - " + redirType + " redirect to " +
                url + " (fetching later)");
          }
          return null;
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(" - " + redirType + " redirect skipped: " +
              (newUrl != null ? "to same url" : "filtered"));
        }
        return null;
      }
    }

    private void queueRedirect(Text redirUrl, FetchItem fit) throws ScoringFilterException {
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
        FetchItemQueue fiq =
          fetchQueues.getFetchItemQueue(fit.queueID);
        fiq.addInProgressFetchItem(fit);
      } else {
        // stop redirecting
        redirecting = false;
        reporter.incrCounter("FetcherStatus", "FetchItem.notCreated.redirect", 1);
      }
    }

    private void logError(Text url, String message) {
      if (LOG.isInfoEnabled()) {
        LOG.info("fetch of " + url + " failed with: " + message);
      }
      errors.incrementAndGet();
    }

    private ParseStatus output(Text key, CrawlDatum datum,
                        Content content, ProtocolStatus pstatus, int status) {

      return output(key, datum, content, pstatus, status, 0);
    }

    private ParseStatus output(Text key, CrawlDatum datum,
                        Content content, ProtocolStatus pstatus, int status, int outlinkDepth) {

      datum.setStatus(status);
      datum.setFetchTime(System.currentTimeMillis());
      if (pstatus != null) datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY, pstatus);
      
      ParseResult parseResult = null;
      if (content != null) {
        Metadata metadata = content.getMetadata();
        
        // store the guessed content type in the crawldatum
        if (content.getContentType() != null) datum.getMetaData().put(new Text(Metadata.CONTENT_TYPE), new Text(content.getContentType()));
        
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
        /* Note: Fetcher will only follow meta-redirects coming from the
         * original URL. */
        if (parsing && status == CrawlDatum.STATUS_FETCH_SUCCESS) {
          if (!skipTruncated || (skipTruncated && !ParseSegment.isTruncated(content))) {
            try {
              parseResult = this.parseUtil.parse(content);
            } catch (Exception e) {
              LOG.warn("Error parsing: " + key + ": " + StringUtils.stringifyException(e));
            }
          }
  
          if (parseResult == null) {
            byte[] signature =
              SignatureFactory.getSignature(getConf()).calculate(content,
                  new ParseStatus().getEmptyParse(conf));
            datum.setSignature(signature);
          }
        }

        /* Store status code in content So we can read this value during
         * parsing (as a separate job) and decide to parse or not.
         */
        content.getMetadata().add(Nutch.FETCH_STATUS_KEY, Integer.toString(status));
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
              parse = parseStatus.getEmptyParse(getConf());
            }

            // Calculate page signature. For non-parsing fetchers this will
            // be done in ParseSegment
            byte[] signature =
              SignatureFactory.getSignature(getConf()).calculate(content, parse);
            // Ensure segment name and score are in parseData metadata
            parseData.getContentMeta().set(Nutch.SEGMENT_NAME_KEY,
                segmentName);
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

            int validCount = 0;

            // Process all outlinks, normalize, filter and deduplicate
            List<Outlink> outlinkList = new ArrayList<Outlink>(outlinksToStore);
            HashSet<String> outlinks = new HashSet<String>(outlinksToStore);
            for (int i = 0; i < links.length && validCount < outlinksToStore; i++) {
              String toUrl = links[i].getToUrl();

              toUrl = ParseOutputFormat.filterNormalize(url.toString(), toUrl, fromHost, ignoreExternalLinks, urlFilters, normalizers);
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
              reporter.incrCounter("FetcherOutlinks", "outlinks_detected", outlinks.size());

              // Counter to limit num outlinks to follow per page
              int outlinkCounter = 0;

              // Calculate variable number of outlinks by depth using the divisor (outlinks = Math.floor(divisor / depth * num.links))
              int maxOutlinksByDepth = (int)Math.floor(outlinksDepthDivisor / (outlinkDepth + 1) * maxOutlinkDepthNumLinks);

              String followUrl;

              // Walk over the outlinks and add as new FetchItem to the queues
              Iterator<String> iter = outlinks.iterator();
              while(iter.hasNext() && outlinkCounter < maxOutlinkDepthNumLinks) {
                followUrl = iter.next();

                // Check whether we'll follow external outlinks
                if (outlinksIgnoreExternal) {
                  if (!URLUtil.getHost(url.toString()).equals(URLUtil.getHost(followUrl))) {
                    continue;
                  }
                }

                reporter.incrCounter("FetcherOutlinks", "outlinks_following", 1);

                // Create new FetchItem with depth incremented
                FetchItem fit = FetchItem.create(new Text(followUrl), new CrawlDatum(CrawlDatum.STATUS_LINKED, interval), queueMode, outlinkDepth + 1);
                fetchQueues.addFetchItem(fit);

                outlinkCounter++;
              }
            }

            // Overwrite the outlinks in ParseData with the normalized and filtered set
            parseData.setOutlinks(outlinkList.toArray(new Outlink[outlinkList.size()]));

            output.collect(url, new NutchWritable(
                    new ParseImpl(new ParseText(parse.getText()),
                                  parseData, parse.isCanonical())));
          }
        }
      } catch (IOException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("fetcher caught:"+e.toString());
        }
      }

      // return parse status if it exits
      if (parseResult != null && !parseResult.isEmpty()) {
        Parse p = parseResult.get(content.getUrl());
        if (p != null) {
          reporter.incrCounter("ParserStatus", ParseStatus.majorCodes[p.getData().getStatus().getMajorCode()], 1);
          return p.getData().getStatus();
        }
      }
      return null;
    }

  }

  public Fetcher() { super(null); }

  public Fetcher(Configuration conf) { super(conf); }

  private void updateStatus(int bytesInPage) throws IOException {
    pages.incrementAndGet();
    bytes.addAndGet(bytesInPage);
  }


  private void reportStatus(int pagesLastSec, int bytesLastSec) throws IOException {
    String status;
    long elapsed = (System.currentTimeMillis() - start)/1000;

    float avgPagesSec = Math.round(((float)pages.get()*10)/elapsed)/10;
    float avgBytesSec = Math.round(((((float)bytes.get())*8)/1000)/elapsed);

    status = activeThreads + " threads, " +
     fetchQueues.getQueueCount() + " queues, "+
     fetchQueues.getTotalSize() + " URLs queued, "+
      pages+" pages, "+errors+" errors, "
      + avgPagesSec + " (" + pagesLastSec + ") pages/s, "
      + avgBytesSec + " (" + bytesLastSec + ") kbits/s, ";

    reporter.setStatus(status);
  }

  public void configure(JobConf job) {
    setConf(job);

    this.segmentName = job.get(Nutch.SEGMENT_NAME_KEY);
    this.storingContent = isStoringContent(job);
    this.parsing = isParsing(job);

//    if (job.getBoolean("fetcher.verbose", false)) {
//      LOG.setLevel(Level.FINE);
//    }
  }

  public void close() {}

  public static boolean isParsing(Configuration conf) {
    return conf.getBoolean("fetcher.parse", true);
  }

  public static boolean isStoringContent(Configuration conf) {
    return conf.getBoolean("fetcher.store.content", true);
  }

  public void run(RecordReader<Text, CrawlDatum> input,
      OutputCollector<Text, NutchWritable> output,
                  Reporter reporter) throws IOException {

    this.output = output;
    this.reporter = reporter;
    this.fetchQueues = new FetchItemQueues(getConf());

    int threadCount = getConf().getInt("fetcher.threads.fetch", 10);
    if (LOG.isInfoEnabled()) { LOG.info("Fetcher: threads: " + threadCount); }

    int timeoutDivisor = getConf().getInt("fetcher.threads.timeout.divisor", 2);
    if (LOG.isInfoEnabled()) { LOG.info("Fetcher: time-out divisor: " + timeoutDivisor); }

    int queueDepthMuliplier =  getConf().getInt("fetcher.queue.depth.multiplier", 50);

    feeder = new QueueFeeder(input, fetchQueues, threadCount * queueDepthMuliplier);
    //feeder.setPriority((Thread.MAX_PRIORITY + Thread.NORM_PRIORITY) / 2);

    // the value of the time limit is either -1 or the time where it should finish
    long timelimit = getConf().getLong("fetcher.timelimit", -1);
    if (timelimit != -1) feeder.setTimeLimit(timelimit);
    feeder.start();

    // set non-blocking & no-robots mode for HTTP protocol plugins.
    getConf().setBoolean(Protocol.CHECK_BLOCKING, false);
    getConf().setBoolean(Protocol.CHECK_ROBOTS, false);

    for (int i = 0; i < threadCount; i++) {       // spawn threads
      new FetcherThread(getConf()).start();
    }

    // select a timeout that avoids a task timeout
    long timeout = getConf().getInt("mapred.task.timeout", 10*60*1000)/timeoutDivisor;

    // Used for threshold check, holds pages and bytes processed in the last second
    int pagesLastSec;
    int bytesLastSec;

    // Set to true whenever the threshold has been exceeded for the first time
    boolean throughputThresholdExceeded = false;
    int throughputThresholdNumRetries = 0;

    int throughputThresholdPages = getConf().getInt("fetcher.throughput.threshold.pages", -1);
    if (LOG.isInfoEnabled()) { LOG.info("Fetcher: throughput threshold: " + throughputThresholdPages); }
    int throughputThresholdMaxRetries = getConf().getInt("fetcher.throughput.threshold.retries", 5);
    if (LOG.isInfoEnabled()) { LOG.info("Fetcher: throughput threshold retries: " + throughputThresholdMaxRetries); }
    long throughputThresholdTimeLimit = getConf().getLong("fetcher.throughput.threshold.check.after", -1);

    do {                                          // wait for threads to exit
      pagesLastSec = pages.get();
      bytesLastSec = (int)bytes.get();

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}

      pagesLastSec = pages.get() - pagesLastSec;
      bytesLastSec = (int)bytes.get() - bytesLastSec;

      reporter.incrCounter("FetcherStatus", "bytes_downloaded", bytesLastSec);

      reportStatus(pagesLastSec, bytesLastSec);

      LOG.info("-activeThreads=" + activeThreads + ", spinWaiting=" + spinWaiting.get()
          + ", fetchQueues.totalSize=" + fetchQueues.getTotalSize());

      if (!feeder.isAlive() && fetchQueues.getTotalSize() < 5) {
        fetchQueues.dump();
      }

      // if throughput threshold is enabled
      if (throughputThresholdTimeLimit < System.currentTimeMillis() && throughputThresholdPages != -1) {
        // Check if we're dropping below the threshold
        if (pagesLastSec < throughputThresholdPages) {
          throughputThresholdNumRetries++;
          LOG.warn(Integer.toString(throughputThresholdNumRetries) + ": dropping below configured threshold of " + Integer.toString(throughputThresholdPages) + " pages per second");

          // Quit if we dropped below threshold too many times
          if (throughputThresholdNumRetries == throughputThresholdMaxRetries) {
            LOG.warn("Dropped below threshold too many times, killing!");

            // Disable the threshold checker
            throughputThresholdPages = -1;

            // Empty the queues cleanly and get number of items that were dropped
            int hitByThrougputThreshold = fetchQueues.emptyQueues();

            if (hitByThrougputThreshold != 0) reporter.incrCounter("FetcherStatus",
              "hitByThrougputThreshold", hitByThrougputThreshold);
          }
        }
      }

      // check timelimit
      if (!feeder.isAlive()) {
        int hitByTimeLimit = fetchQueues.checkTimelimit();
        if (hitByTimeLimit != 0) reporter.incrCounter("FetcherStatus",
            "hitByTimeLimit", hitByTimeLimit);
      }

      // some requests seem to hang, despite all intentions
      if ((System.currentTimeMillis() - lastRequestStart.get()) > timeout) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Aborting with "+activeThreads+" hung threads.");
        }
        return;
      }

    } while (activeThreads.get() > 0);
    LOG.info("-activeThreads=" + activeThreads);

  }

  public void fetch(Path segment, int threads)
    throws IOException {

    checkConfiguration();

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("Fetcher: starting at " + sdf.format(start));
      LOG.info("Fetcher: segment: " + segment);
    }

    // set the actual time for the timelimit relative
    // to the beginning of the whole job and not of a specific task
    // otherwise it keeps trying again if a task fails
    long timelimit = getConf().getLong("fetcher.timelimit.mins", -1);
    if (timelimit != -1) {
      timelimit = System.currentTimeMillis() + (timelimit * 60 * 1000);
      LOG.info("Fetcher Timelimit set for : " + timelimit);
      getConf().setLong("fetcher.timelimit", timelimit);
    }

    // Set the time limit after which the throughput threshold feature is enabled
    timelimit = getConf().getLong("fetcher.throughput.threshold.check.after", 10);
    timelimit = System.currentTimeMillis() + (timelimit * 60 * 1000);
    getConf().setLong("fetcher.throughput.threshold.check.after", timelimit);

    int maxOutlinkDepth = getConf().getInt("fetcher.follow.outlinks.depth", -1);
    if (maxOutlinkDepth > 0) {
      LOG.info("Fetcher: following outlinks up to depth: " + Integer.toString(maxOutlinkDepth));

      int maxOutlinkDepthNumLinks = getConf().getInt("fetcher.follow.outlinks.num.links", 4);
      int outlinksDepthDivisor = getConf().getInt("fetcher.follow.outlinks.depth.divisor", 2);

      int totalOutlinksToFollow = 0;
      for (int i = 0; i < maxOutlinkDepth; i++) {
        totalOutlinksToFollow += (int)Math.floor(outlinksDepthDivisor / (i + 1) * maxOutlinkDepthNumLinks);
      }

      LOG.info("Fetcher: maximum outlinks to follow: " + Integer.toString(totalOutlinksToFollow));
    }

    JobConf job = new NutchJob(getConf());
    job.setJobName("fetch " + segment);

    job.setInt("fetcher.threads.fetch", threads);
    job.set(Nutch.SEGMENT_NAME_KEY, segment.getName());

    // for politeness, don't permit parallel execution of a single task
    job.setSpeculativeExecution(false);

    FileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.GENERATE_DIR_NAME));
    job.setInputFormat(InputFormat.class);

    job.setMapRunnerClass(Fetcher.class);

    FileOutputFormat.setOutputPath(job, segment);
    job.setOutputFormat(FetcherOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NutchWritable.class);

    JobClient.runJob(job);

    long end = System.currentTimeMillis();
    LOG.info("Fetcher: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
  }


  /** Run the fetcher. */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new Fetcher(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {

    String usage = "Usage: Fetcher <segment> [-threads n]";

    if (args.length < 1) {
      System.err.println(usage);
      return -1;
    }

    Path segment = new Path(args[0]);

    int threads = getConf().getInt("fetcher.threads.fetch", 10);
    boolean parsing = false;

    for (int i = 1; i < args.length; i++) {       // parse command line
      if (args[i].equals("-threads")) {           // found -threads option
        threads =  Integer.parseInt(args[++i]);
      }
    }

    getConf().setInt("fetcher.threads.fetch", threads);

    try {
      fetch(segment, threads);
      return 0;
    } catch (Exception e) {
      LOG.error("Fetcher: " + StringUtils.stringifyException(e));
      return -1;
    }

  }

  private void checkConfiguration() {

    // ensure that a value has been set for the agent name and that that
    // agent name is the first value in the agents we advertise for robot
    // rules parsing
    String agentName = getConf().get("http.agent.name");
    if (agentName == null || agentName.trim().length() == 0) {
      String message = "Fetcher: No agents listed in 'http.agent.name'"
          + " property.";
      if (LOG.isErrorEnabled()) {
        LOG.error(message);
      }
      throw new IllegalArgumentException(message);
    } else {

      // get all of the agents that we advertise
      String agentNames = getConf().get("http.robots.agents");
      StringTokenizer tok = new StringTokenizer(agentNames, ",");
      ArrayList<String> agents = new ArrayList<String>();
      while (tok.hasMoreTokens()) {
        agents.add(tok.nextToken().trim());
      }

      // if the first one is not equal to our agent name, log fatal and throw
      // an exception
      if (!(agents.get(0)).equalsIgnoreCase(agentName)) {
        String message = "Fetcher: Your 'http.agent.name' value should be "
            + "listed first in 'http.robots.agents' property.";
        if (LOG.isWarnEnabled()) {
          LOG.warn(message);
        }
      }
    }
  }

}
