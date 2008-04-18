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

package org.apache.nutch.fetcher;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.*;


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
public class Fetcher2 extends Configured implements MapRunnable { 

  public static final Log LOG = LogFactory.getLog(Fetcher2.class);
  
  public static class InputFormat extends SequenceFileInputFormat {
    /** Don't split inputs, to keep things polite. */
    public InputSplit[] getSplits(JobConf job, int nSplits)
      throws IOException {
      Path[] files = listPaths(job);
      FileSplit[] splits = new FileSplit[files.length];
      FileSystem fs = FileSystem.get(job);
      for (int i = 0; i < files.length; i++) {
        splits[i] = new FileSplit(files[i], 0, fs.getFileStatus(files[i]).getLen(), job);
      }
      return splits;
    }
  }

  private OutputCollector output;
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
    String queueID;
    Text url;
    URL u;
    CrawlDatum datum;
    
    public FetchItem(Text url, URL u, CrawlDatum datum, String queueID) {
      this.url = url;
      this.u = u;
      this.datum = datum;
      this.queueID = queueID;
    }
    
    /** Create an item. Queue id will be created based on <code>byIP</code>
     * argument, either as a protocol + hostname pair, or protocol + IP
     * address pair.
     */
    public static FetchItem create(Text url, CrawlDatum datum, boolean byIP) {
      String queueID;
      URL u = null;
      try {
        u = new URL(url.toString());
      } catch (Exception e) {
        LOG.warn("Cannot parse url: " + url, e);
        return null;
      }
      String proto = u.getProtocol().toLowerCase();
      String host;
      if (byIP) {
        try {
          InetAddress addr = InetAddress.getByName(u.getHost());
          host = addr.getHostAddress();
        } catch (UnknownHostException e) {
          // unable to resolve it, so don't fall back to host name
          LOG.warn("Unable to resolve: " + u.getHost() + ", skipping.");
          return null;
        }
      } else {
        host = u.getHost();
        if (host == null) {
          LOG.warn("Unknown host for url: " + url + ", skipping.");
          return null;
        }
        host = host.toLowerCase();
      }
      queueID = proto + "://" + host;
      return new FetchItem(url, u, datum, queueID);
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
    Set<FetchItem>  inProgress = Collections.synchronizedSet(new HashSet<FetchItem>());
    AtomicLong nextFetchTime = new AtomicLong();
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
    
    public int getQueueSize() {
      return queue.size();
    }
    
    public int getInProgressSize() {
      return inProgress.size();
    }
    
    public void finishFetchItem(FetchItem it, boolean asap) {
      if (it != null) {
        inProgress.remove(it);
        setEndTime(System.currentTimeMillis(), asap);
      }
    }
    
    public void addFetchItem(FetchItem it) {
      if (it == null) return;
      queue.add(it);
    }
    
    public void addInProgressFetchItem(FetchItem it) {
      if (it == null) return;
      inProgress.add(it);
    }
    
    public FetchItem getFetchItem() {
      if (inProgress.size() >= maxThreads) return null;
      long now = System.currentTimeMillis();
      if (nextFetchTime.get() > now) return null;
      FetchItem it = null;
      if (queue.size() == 0) return null;
      try {
        it = queue.remove(0);
        inProgress.add(it);
      } catch (Exception e) {
        LOG.error("Cannot remove FetchItem from queue or cannot add it to inProgress queue", e);
      }
      return it;
    }
    
    public synchronized void dump() {
      LOG.info("  maxThreads    = " + maxThreads);
      LOG.info("  inProgress    = " + inProgress.size());
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
    boolean byIP;
    long crawlDelay;
    long minCrawlDelay;
    Configuration conf;    
    
    public FetchItemQueues(Configuration conf) {
      this.conf = conf;
      this.maxThreads = conf.getInt("fetcher.threads.per.host", 1);
      // backward-compatible default setting
      this.byIP = conf.getBoolean("fetcher.threads.per.host.by.ip", false);
      this.crawlDelay = (long) (conf.getFloat("fetcher.server.delay", 1.0f) * 1000);
      this.minCrawlDelay = (long) (conf.getFloat("fetcher.server.min.delay", 0.0f) * 1000);
    }
    
    public int getTotalSize() {
      return totalSize.get();
    }
    
    public int getQueueCount() {
      return queues.size();
    }
    
    public void addFetchItem(Text url, CrawlDatum datum) {
      FetchItem it = FetchItem.create(url, datum, byIP);
      if (it != null) addFetchItem(it);
    }
    
    public void addFetchItem(FetchItem it) {
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
    private RecordReader reader;
    private FetchItemQueues queues;
    private int size;
    
    public QueueFeeder(RecordReader reader, FetchItemQueues queues, int size) {
      this.reader = reader;
      this.queues = queues;
      this.size = size;
      this.setDaemon(true);
      this.setName("QueueFeeder");
    }
    
    public void run() {
      boolean hasMore = true;
      int cnt = 0;
      
      while (hasMore) {
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
              LOG.fatal("QueueFeeder error reading input, record " + cnt, e);
              return;
            }
          }
        }
      }
      LOG.info("QueueFeeder finished: total " + cnt + " records.");
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
    private boolean byIP;
    private int maxRedirect;
    private String reprUrl;
    private boolean redirecting;
    private int redirectCount;

    public FetcherThread(Configuration conf) {
      this.setDaemon(true);                       // don't hang JVM on exit
      this.setName("FetcherThread");              // use an informative name
      this.conf = conf;
      this.urlFilters = new URLFilters(conf);
      this.scfilters = new ScoringFilters(conf);
      this.parseUtil = new ParseUtil(conf);
      this.protocolFactory = new ProtocolFactory(conf);
      this.normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_FETCHER);
      this.maxCrawlDelay = conf.getInt("fetcher.max.crawl.delay", 30) * 1000;
      // backward-compatible default setting
      this.byIP = conf.getBoolean("fetcher.threads.per.host.by.ip", true);
      this.maxRedirect = conf.getInt("http.redirect.max", 3);
    }

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
            if (LOG.isInfoEnabled()) { LOG.info("fetching " + fit.url); }

            // fetch the page
            redirecting = false;
            redirectCount = 0;
            do {
              if (LOG.isDebugEnabled()) {
                LOG.debug("redirectCount=" + redirectCount);
              }
              redirecting = false;
              Protocol protocol = this.protocolFactory.getProtocol(fit.url.toString());
              RobotRules rules = protocol.getRobotRules(fit.url, fit.datum);
              if (!rules.isAllowed(fit.u)) {
                // unblock
                fetchQueues.finishFetchItem(fit, true);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Denied by robots.txt: " + fit.url);
                }
                output(fit.url, fit.datum, null, ProtocolStatus.STATUS_ROBOTS_DENIED, CrawlDatum.STATUS_FETCH_GONE);
                continue;
              }
              if (rules.getCrawlDelay() > 0) {
                if (rules.getCrawlDelay() > maxCrawlDelay) {
                  // unblock
                  fetchQueues.finishFetchItem(fit, true);
                  LOG.debug("Crawl-Delay for " + fit.url + " too long (" + rules.getCrawlDelay() + "), skipping");
                  output(fit.url, fit.datum, null, ProtocolStatus.STATUS_ROBOTS_DENIED, CrawlDatum.STATUS_FETCH_GONE);
                  continue;
                } else {
                  FetchItemQueue fiq = fetchQueues.getFetchItemQueue(fit.queueID);
                  fiq.crawlDelay = rules.getCrawlDelay();
                }
              }
              ProtocolOutput output = protocol.getProtocolOutput(fit.url, fit.datum);
              ProtocolStatus status = output.getStatus();
              Content content = output.getContent();
              ParseStatus pstatus = null;
              // unblock queue
              fetchQueues.finishFetchItem(fit);

              String urlString = fit.url.toString();

              switch(status.getCode()) {
                
              case ProtocolStatus.WOULDBLOCK:
                // retry ?
                fetchQueues.addFetchItem(fit);
                break;

              case ProtocolStatus.SUCCESS:        // got a page
                pstatus = output(fit.url, fit.datum, content, status, CrawlDatum.STATUS_FETCH_SUCCESS);
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
                    CrawlDatum newDatum = new CrawlDatum(CrawlDatum.STATUS_DB_UNFETCHED,
                        fit.datum.getFetchInterval(), fit.datum.getScore());
                    if (reprUrl != null) {
                      newDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY,
                          new Text(reprUrl));
                    }
                    fit = FetchItem.create(redirUrl, newDatum, byIP);
                    if (fit != null) {
                      FetchItemQueue fiq =
                        fetchQueues.getFetchItemQueue(fit.queueID);
                      fiq.addInProgressFetchItem(fit);
                    } else {
                      // stop redirecting
                      redirecting = false;
                    }
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
                  CrawlDatum newDatum = new CrawlDatum(CrawlDatum.STATUS_DB_UNFETCHED,
                      fit.datum.getFetchInterval(), fit.datum.getScore());
                  if (reprUrl != null) {
                    newDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY,
                        new Text(reprUrl));
                  }
                  fit = FetchItem.create(redirUrl, newDatum, byIP);
                  if (fit != null) {
                    FetchItemQueue fiq =
                      fetchQueues.getFetchItemQueue(fit.queueID);
                    fiq.addInProgressFetchItem(fit);
                  } else {
                    // stop redirecting
                    redirecting = false;
                  }
                } else {
                  // stop redirecting
                  redirecting = false;
                }
                break;

              case ProtocolStatus.EXCEPTION:
                logError(fit.url, status.getMessage());
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

              if (redirecting && redirectCount >= maxRedirect) {
                fetchQueues.finishFetchItem(fit);
                if (LOG.isInfoEnabled()) {
                  LOG.info(" - redirect count exceeded " + fit.url);
                }
                output(fit.url, fit.datum, null, ProtocolStatus.STATUS_REDIR_EXCEEDED, CrawlDatum.STATUS_FETCH_GONE);
              }

            } while (redirecting && (redirectCount < maxRedirect));
            
          } catch (Throwable t) {                 // unexpected exception
            // unblock
            fetchQueues.finishFetchItem(fit);
            logError(fit.url, t.toString());
            output(fit.url, fit.datum, null, ProtocolStatus.STATUS_FAILED, CrawlDatum.STATUS_FETCH_RETRY);
          }
        }

      } catch (Throwable e) {
        if (LOG.isFatalEnabled()) {
          e.printStackTrace(LogUtil.getFatalStream(LOG));
          LOG.fatal("fetcher caught:"+e.toString());
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
          CrawlDatum newDatum = new CrawlDatum();
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

    private void logError(Text url, String message) {
      if (LOG.isInfoEnabled()) {
        LOG.info("fetch of " + url + " failed with: " + message);
      }
      errors.incrementAndGet();
    }

    private ParseStatus output(Text key, CrawlDatum datum,
                        Content content, ProtocolStatus pstatus, int status) {

      datum.setStatus(status);
      datum.setFetchTime(System.currentTimeMillis());
      if (pstatus != null) datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY, pstatus);

      ParseResult parseResult = null;
      if (content != null) {
        Metadata metadata = content.getMetadata();
        // add segment to metadata
        metadata.set(Nutch.SEGMENT_NAME_KEY, segmentName);
        // add score to content metadata so that ParseSegment can pick it up.
        try {
          scfilters.passScoreBeforeParsing(key, datum, content);
        } catch (Exception e) {
          if (LOG.isWarnEnabled()) {
            e.printStackTrace(LogUtil.getWarnStream(LOG));
            LOG.warn("Couldn't pass score, url " + key + " (" + e + ")");
          }
        }
        /* Note: Fetcher will only follow meta-redirects coming from the
         * original URL. */ 
        if (parsing && status == CrawlDatum.STATUS_FETCH_SUCCESS) {
          try {
            parseResult = this.parseUtil.parse(content);
          } catch (Exception e) {
            LOG.warn("Error parsing: " + key + ": " + StringUtils.stringifyException(e));
          }

          if (parseResult == null) {
            byte[] signature = 
              SignatureFactory.getSignature(getConf()).calculate(content, 
                  new ParseStatus().getEmptyParse(conf));
            datum.setSignature(signature);
          }
        }
        
        if (!parsing) {
          /* Store status code in content if we are not parsing in fetcher. 
           * So we can read this value during parsing (as a separate job) and
           * decide to parse or not.
           * */
          content.getMetadata().add(Nutch.FETCH_STATUS_KEY, Integer.toString(status));
        }
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
            
            if (!parseStatus.isSuccess()) {
              LOG.warn("Error parsing: " + key + ": " + parseStatus);
              parse = parseStatus.getEmptyParse(getConf());
            }

            // Calculate page signature. For non-parsing fetchers this will
            // be done in ParseSegment
            byte[] signature = 
              SignatureFactory.getSignature(getConf()).calculate(content, parse);
            // Ensure segment name and score are in parseData metadata
            parse.getData().getContentMeta().set(Nutch.SEGMENT_NAME_KEY, 
                segmentName);
            parse.getData().getContentMeta().set(Nutch.SIGNATURE_KEY, 
                StringUtil.toHexString(signature));
            // Pass fetch time to content meta
            parse.getData().getContentMeta().set(Nutch.FETCH_TIME_KEY,
                Long.toString(datum.getFetchTime()));
            if (url.equals(key))
              datum.setSignature(signature);
            try {
              scfilters.passScoreAfterParsing(url, content, parse);
            } catch (Exception e) {
              if (LOG.isWarnEnabled()) {
                e.printStackTrace(LogUtil.getWarnStream(LOG));
                LOG.warn("Couldn't pass score, url " + key + " (" + e + ")");
              }
            }
            output.collect(url, new NutchWritable(
                    new ParseImpl(new ParseText(parse.getText()), 
                                  parse.getData(), parse.isCanonical())));
          }
        }
      } catch (IOException e) {
        if (LOG.isFatalEnabled()) {
          e.printStackTrace(LogUtil.getFatalStream(LOG));
          LOG.fatal("fetcher caught:"+e.toString());
        }
      }

      // return parse status if it exits
      if (parseResult != null && !parseResult.isEmpty()) {
        Parse p = parseResult.get(content.getUrl());
        if (p != null) {
          return p.getData().getStatus();
        }
      }
      return null;
    }
    
  }

  public Fetcher2() { super(null); }

  public Fetcher2(Configuration conf) { super(conf); }

  private void updateStatus(int bytesInPage) throws IOException {
    pages.incrementAndGet();
    bytes.addAndGet(bytesInPage);
  }

  
  private void reportStatus() throws IOException {
    String status;
    long elapsed = (System.currentTimeMillis() - start)/1000;
    status = activeThreads + " threads, " +
      pages+" pages, "+errors+" errors, "
      + Math.round(((float)pages.get()*10)/elapsed)/10.0+" pages/s, "
      + Math.round(((((float)bytes.get())*8)/1024)/elapsed)+" kb/s, ";
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

  public void run(RecordReader input, OutputCollector output,
                  Reporter reporter) throws IOException {

    this.output = output;
    this.reporter = reporter;
    this.fetchQueues = new FetchItemQueues(getConf());

    int threadCount = getConf().getInt("fetcher.threads.fetch", 10);
    if (LOG.isInfoEnabled()) { LOG.info("Fetcher: threads: " + threadCount); }

    feeder = new QueueFeeder(input, fetchQueues, threadCount * 50);
    //feeder.setPriority((Thread.MAX_PRIORITY + Thread.NORM_PRIORITY) / 2);
    feeder.start();

    // set non-blocking & no-robots mode for HTTP protocol plugins.
    getConf().setBoolean(Protocol.CHECK_BLOCKING, false);
    getConf().setBoolean(Protocol.CHECK_ROBOTS, false);
    
    for (int i = 0; i < threadCount; i++) {       // spawn threads
      new FetcherThread(getConf()).start();
    }

    // select a timeout that avoids a task timeout
    long timeout = getConf().getInt("mapred.task.timeout", 10*60*1000)/2;

    do {                                          // wait for threads to exit
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}

      reportStatus();
      LOG.info("-activeThreads=" + activeThreads + ", spinWaiting=" + spinWaiting.get()
          + ", fetchQueues.totalSize=" + fetchQueues.getTotalSize());

      if (!feeder.isAlive() && fetchQueues.getTotalSize() < 5) {
        fetchQueues.dump();
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

  public void fetch(Path segment, int threads, boolean parsing)
    throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("Fetcher: starting");
      LOG.info("Fetcher: segment: " + segment);
    }

    JobConf job = new NutchJob(getConf());
    job.setJobName("fetch " + segment);

    job.setInt("fetcher.threads.fetch", threads);
    job.set(Nutch.SEGMENT_NAME_KEY, segment.getName());
    job.setBoolean("fetcher.parse", parsing);

    // for politeness, don't permit parallel execution of a single task
    job.setSpeculativeExecution(false);

    job.setInputPath(new Path(segment, CrawlDatum.GENERATE_DIR_NAME));
    job.setInputFormat(InputFormat.class);

    job.setMapRunnerClass(Fetcher2.class);

    job.setOutputPath(segment);
    job.setOutputFormat(FetcherOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NutchWritable.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) { LOG.info("Fetcher: done"); }
  }


  /** Run the fetcher. */
  public static void main(String[] args) throws Exception {

    String usage = "Usage: Fetcher <segment> [-threads n] [-noParsing]";

    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    Path segment = new Path(args[0]);

    Configuration conf = NutchConfiguration.create();

    int threads = conf.getInt("fetcher.threads.fetch", 10);
    boolean parsing = true;

    for (int i = 1; i < args.length; i++) {       // parse command line
      if (args[i].equals("-threads")) {           // found -threads option
        threads =  Integer.parseInt(args[++i]);
      } else if (args[i].equals("-noParsing")) parsing = false;
    }

    conf.setInt("fetcher.threads.fetch", threads);
    if (!parsing) {
      conf.setBoolean("fetcher.parse", parsing);
    }
    Fetcher2 fetcher = new Fetcher2(conf);          // make a Fetcher
    
    fetcher.fetch(segment, threads, parsing);              // run the Fetcher

  }
}
