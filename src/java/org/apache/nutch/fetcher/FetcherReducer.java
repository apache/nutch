/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.fetcher;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.URLWebPage;
import org.apache.nutch.host.HostDb;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatusCodes;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.protocol.RobotRules;
import org.apache.nutch.storage.Host;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;

import crawlercommons.robots.BaseRobotRules;

public class FetcherReducer
extends GoraReducer<IntWritable, FetchEntry, String, WebPage> {

  public static final Logger LOG = FetcherJob.LOG;

  private final AtomicInteger activeThreads = new AtomicInteger(0);
  private final AtomicInteger spinWaiting = new AtomicInteger(0);

  private final long start = System.currentTimeMillis(); // start time of fetcher run
  private final AtomicLong lastRequestStart = new AtomicLong(start);

  private final AtomicLong bytes = new AtomicLong(0);        // total bytes fetched
  private final AtomicInteger pages = new AtomicInteger(0);  // total pages fetched
  private final AtomicInteger errors = new AtomicInteger(0); // total pages errored

  private QueueFeeder feeder;

  private final List<FetcherThread> fetcherThreads = new ArrayList<FetcherThread>();

  private FetchItemQueues fetchQueues;

  private boolean storingContent;
  private boolean parse;

  private ParseUtil parseUtil;
  private boolean skipTruncated;

  /**
   * This class described the item to be fetched.
   */
  private static class FetchItem {
    WebPage page;
    String queueID;
    String url;
    URL u;

    public FetchItem(String url, WebPage page, URL u, String queueID) {
      this.page = page;
      this.url = url;
      this.u = u;
      this.queueID = queueID;
    }

    /** Create an item. Queue id will be created based on <code>queueMode</code>
     * argument, either as a protocol + hostname pair, protocol + IP
     * address pair or protocol+domain pair.
     */
    public static FetchItem create(String url, WebPage page, String queueMode) {
      String queueID;
      URL u = null;
      try {
        u = new URL(url);
      } catch (final Exception e) {
        LOG.warn("Cannot parse url: " + url, e);
        return null;
      }
      final String proto = u.getProtocol().toLowerCase();
      String host;
      if (FetchItemQueues.QUEUE_MODE_IP.equalsIgnoreCase(queueMode)) {
        try {
          final InetAddress addr = InetAddress.getByName(u.getHost());
          host = addr.getHostAddress();
        } catch (final UnknownHostException e) {
          // unable to resolve it, so don't fall back to host name
          LOG.warn("Unable to resolve: " + u.getHost() + ", skipping.");
          return null;
        }
      }
      else if (FetchItemQueues.QUEUE_MODE_DOMAIN.equalsIgnoreCase(queueMode)){
        host = URLUtil.getDomainName(u);
        if (host == null) {
          LOG.warn("Unknown domain for url: " + url + ", using URL string as key");
          host=u.toExternalForm();
        }
      }
      else {
        host = u.getHost();
        if (host == null) {
          LOG.warn("Unknown host for url: " + url + ", using URL string as key");
          host=u.toExternalForm();
        }
      }
      queueID = proto + "://" + host.toLowerCase();
      return new FetchItem(url, page, u, queueID);
    }

    @Override
    public String toString() {
      return "FetchItem [queueID=" + queueID + ", url=" + url + ", u=" + u
          + ", page=" + page + "]";
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

    public FetchItemQueue(Configuration conf, int maxThreads, long crawlDelay, long minCrawlDelay) {
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

    @SuppressWarnings("unused")
    public void addInProgressFetchItem(FetchItem it) {
      if (it == null) return;
      inProgress.add(it);
    }

    public FetchItem getFetchItem() {
      if (inProgress.size() >= maxThreads) return null;
      final long now = System.currentTimeMillis();
      if (nextFetchTime.get() > now) return null;
      FetchItem it = null;
      if (queue.size() == 0) return null;
      try {
        it = queue.remove(0);
        inProgress.add(it);
      } catch (final Exception e) {
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
        final FetchItem it = queue.get(i);
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
    
    public synchronized int emptyQueue() {
      int presize = queue.size();
      queue.clear();
      return presize;
    }
  }

  /**
   * Convenience class - a collection of queues that keeps track of the total
   * number of items, and provides items eligible for fetching from any queue.
   */
  private static class FetchItemQueues {
    @SuppressWarnings("unused")
    public static final String DEFAULT_ID = "default";
    Map<String, FetchItemQueue> queues = new HashMap<String, FetchItemQueue>();
    AtomicInteger totalSize = new AtomicInteger(0);
    int maxThreads;
    String queueMode;
    long crawlDelay;
    long minCrawlDelay;
    Configuration conf;
    long timelimit = -1;
    
    boolean useHostSettings = false;
    HostDb hostDb = null;

    public static final String QUEUE_MODE_HOST = "byHost";
    public static final String QUEUE_MODE_DOMAIN = "byDomain";
    public static final String QUEUE_MODE_IP = "byIP";

    public FetchItemQueues(Configuration conf) throws IOException {
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
      
      // Optionally enable host specific queue behavior 
      if (queueMode.equals(QUEUE_MODE_HOST)) {
        useHostSettings = conf.getBoolean("fetcher.queue.use.host.settings", false);
        if (useHostSettings) {
          LOG.info("Host specific queue settings enabled.");
          // Initialize the HostDb if we need it.
          hostDb = new HostDb(conf);
        }
      }
      
      this.crawlDelay = (long) (conf.getFloat("fetcher.server.delay", 1.0f) * 1000);
      this.minCrawlDelay = (long) (conf.getFloat("fetcher.server.min.delay", 0.0f) * 1000);
      this.timelimit = conf.getLong("fetcher.timelimit", -1);
    }

    public int getTotalSize() {
      return totalSize.get();
    }

    public int getQueueCount() {
      return queues.size();
    }

    public void addFetchItem(String url, WebPage page) {
      final FetchItem it = FetchItem.create(url, page, queueMode);
      if (it != null) addFetchItem(it);
    }

    public synchronized void addFetchItem(FetchItem it) {
      final FetchItemQueue fiq = getFetchItemQueue(it.queueID);
      fiq.addFetchItem(it);
      totalSize.incrementAndGet();
    }

    public void finishFetchItem(FetchItem it) {
      finishFetchItem(it, false);
    }

    public void finishFetchItem(FetchItem it, boolean asap) {
      final FetchItemQueue fiq = queues.get(it.queueID);
      if (fiq == null) {
        LOG.warn("Attempting to finish item from unknown queue: " + it);
        return;
      }
      fiq.finishFetchItem(it, asap);
    }

    public synchronized FetchItemQueue getFetchItemQueue(String id) {
      FetchItemQueue fiq = queues.get(id);
      if (fiq == null) {
        // Create a new queue
        if (useHostSettings) {
          // Use host specific queue settings (if defined in the host table)
          try {
            String hostname = id.substring(id.indexOf("://")+3);
            Host host = hostDb.getByHostName(hostname);
            if (host != null) {
              fiq = new FetchItemQueue(conf,
                                       host.getInt("q_mt", maxThreads),
                                       host.getLong("q_cd", crawlDelay),
                                       host.getLong("q_mcd", minCrawlDelay));
            }
            
          } catch (IOException e) {
            LOG.error("Error while trying to access host settings", e);
          }
        } 
        if (fiq == null) {
          // Use queue defaults
          fiq = new FetchItemQueue(conf, maxThreads, crawlDelay, minCrawlDelay);
        }
        queues.put(id, fiq);
      }
      return fiq;
    }

    public synchronized FetchItem getFetchItem() {
      final Iterator<Map.Entry<String, FetchItemQueue>> it =
        queues.entrySet().iterator();
      while (it.hasNext()) {
        final FetchItemQueue fiq = it.next().getValue();
        // reap empty queues
        if (fiq.getQueueSize() == 0 && fiq.getInProgressSize() == 0) {
          it.remove();
          continue;
        }
        final FetchItem fit = fiq.getFetchItem();
        if (fit != null) {
          totalSize.decrementAndGet();

          return fit;
        }
      }
      return null;
    }
    
    public synchronized int checkTimelimit() {
      if (System.currentTimeMillis() >= timelimit && timelimit != -1) {
        return emptyQueues();
      }
      return 0;
    }
    

    public synchronized void dump() {
      for (final String id : queues.keySet()) {
        final FetchItemQueue fiq = queues.get(id);
        if (fiq.getQueueSize() == 0) continue;
        LOG.info("* queue: " + id);
        fiq.dump();
      }
    }

    // empties the queues (used by timebomb and throughput threshold)
    public synchronized int emptyQueues() {
      int count = 0;
      
      // emptying the queues
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
      // there might also be a case where totalsize !=0 but number of queues
      // == 0
      // in which case we simply force it to 0 to avoid blocking
      if (totalSize.get() != 0 && queues.size() == 0) totalSize.set(0);

      return count;
    }
  }

  /**
   * This class picks items from queues and fetches the pages.
   */
  private class FetcherThread extends Thread {
    private final URLFilters urlFilters;
    private final URLNormalizers normalizers;
    private final ProtocolFactory protocolFactory;
    private final long maxCrawlDelay;
    @SuppressWarnings("unused")
    private final boolean byIP;
    private String reprUrl;

    private final Context context;
    private final boolean ignoreExternalLinks;

    public FetcherThread(Context context, int num) {
      this.setDaemon(true);                       // don't hang JVM on exit
      this.setName("FetcherThread" + num);        // use an informative name
      this.context = context;
      Configuration conf = context.getConfiguration();
      this.urlFilters = new URLFilters(conf);
      this.protocolFactory = new ProtocolFactory(conf);
      this.normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_FETCHER);
      this.maxCrawlDelay = conf.getInt("fetcher.max.crawl.delay", 30) * 1000;
      // backward-compatible default setting
      this.byIP = conf.getBoolean("fetcher.threads.per.host.by.ip", true);
      this.ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);
    }

    @Override
    @SuppressWarnings("fallthrough")
    public void run() {
      activeThreads.incrementAndGet(); // count threads

      FetchItem fit = null;
      try {

        while (true) {
          fit = fetchQueues.getFetchItem();
          if (fit == null) {
            if (feeder.isAlive() || fetchQueues.getTotalSize() > 0) {
              if (LOG.isDebugEnabled()) {
                LOG.debug(getName() + " fetchQueues.getFetchItem() was null, spin-waiting ...");
              }
              // spin-wait.
              spinWaiting.incrementAndGet();
              try {
                Thread.sleep(500);
              } catch (final Exception e) {}
              spinWaiting.decrementAndGet();
              continue;
            } else {
              // all done, finish this thread
              return;
            }
          }
          lastRequestStart.set(System.currentTimeMillis());
          if (!fit.page.isReadable(WebPage.Field.REPR_URL.getIndex())) {
            reprUrl = fit.url;
          } else {
            reprUrl = TableUtil.toString(fit.page.getReprUrl());
          }
          try {
            LOG.info("fetching " + fit.url + " (queue crawl delay=" + 
                      fetchQueues.getFetchItemQueue(fit.queueID).crawlDelay + "ms)"); 

            // fetch the page
            final Protocol protocol = this.protocolFactory.getProtocol(fit.url);
            final BaseRobotRules rules = protocol.getRobotRules(fit.url, fit.page);
            if (!rules.isAllowed(fit.u.toString())) {
              // unblock
              fetchQueues.finishFetchItem(fit, true);
              if (LOG.isDebugEnabled()) {
                LOG.debug("Denied by robots.txt: " + fit.url);
              }
              output(fit, null, ProtocolStatusUtils.STATUS_ROBOTS_DENIED,
                  CrawlStatus.STATUS_GONE);
              continue;
            }
            if (rules.getCrawlDelay() > 0) {
              if (rules.getCrawlDelay() > maxCrawlDelay && maxCrawlDelay >= 0) {
                // unblock
                fetchQueues.finishFetchItem(fit, true);
                LOG.debug("Crawl-Delay for " + fit.url + " too long (" + rules.getCrawlDelay() + "), skipping");
                output(fit, null, ProtocolStatusUtils.STATUS_ROBOTS_DENIED, CrawlStatus.STATUS_GONE);
                continue;
              } else {
                final FetchItemQueue fiq = fetchQueues.getFetchItemQueue(fit.queueID);
                fiq.crawlDelay = rules.getCrawlDelay();
                if (LOG.isDebugEnabled()) {
                  LOG.info("Crawl delay for queue: " + fit.queueID + " is set to " + fiq.crawlDelay + " as per robots.txt. url: " + fit.url);
                }
              }
            }
            final ProtocolOutput output = protocol.getProtocolOutput(fit.url, fit.page);
            final ProtocolStatus status = output.getStatus();
            final Content content = output.getContent();
            // unblock queue
            fetchQueues.finishFetchItem(fit);

            context.getCounter("FetcherStatus", ProtocolStatusUtils.getName(status.getCode())).increment(1);

            int length = 0;
            if (content!=null && content.getContent()!=null) length= content.getContent().length;
            updateStatus(length);

            switch(status.getCode()) {

            case ProtocolStatusCodes.WOULDBLOCK:
              // retry ?
              fetchQueues.addFetchItem(fit);
              break;

            case ProtocolStatusCodes.SUCCESS:        // got a page
              output(fit, content, status, CrawlStatus.STATUS_FETCHED);
              break;

            case ProtocolStatusCodes.MOVED:         // redirect
            case ProtocolStatusCodes.TEMP_MOVED:
              byte code;
              boolean temp;
              if (status.getCode() == ProtocolStatusCodes.MOVED) {
                code = CrawlStatus.STATUS_REDIR_PERM;
                temp = false;
              } else {
                code = CrawlStatus.STATUS_REDIR_TEMP;
                temp = true;
              }
              final String newUrl = ProtocolStatusUtils.getMessage(status);
              handleRedirect(fit.url, newUrl, temp,  FetcherJob.PROTOCOL_REDIR, fit.page);
              output(fit, content, status, code);
              break;
            case ProtocolStatusCodes.EXCEPTION:
              logFetchFailure(fit.url, ProtocolStatusUtils.getMessage(status));
              /* FALLTHROUGH */
            case ProtocolStatusCodes.RETRY:          // retry
            case ProtocolStatusCodes.BLOCKED:
              output(fit, null, status, CrawlStatus.STATUS_RETRY);
              break;

            case ProtocolStatusCodes.GONE:           // gone
            case ProtocolStatusCodes.NOTFOUND:
            case ProtocolStatusCodes.ACCESS_DENIED:
            case ProtocolStatusCodes.ROBOTS_DENIED:
              output(fit, null, status, CrawlStatus.STATUS_GONE);
              break;

            case ProtocolStatusCodes.NOTMODIFIED:
              output(fit, null, status, CrawlStatus.STATUS_NOTMODIFIED);
              break;

            default:
              if (LOG.isWarnEnabled()) {
                LOG.warn("Unknown ProtocolStatus: " + status.getCode());
              }
              output(fit, null, status, CrawlStatus.STATUS_RETRY);
            }

          } catch (final Throwable t) {                 // unexpected exception
            // unblock
            fetchQueues.finishFetchItem(fit);
            LOG.error("Unexpected error for " + fit.url, t);
            output(fit, null, ProtocolStatusUtils.STATUS_FAILED,
                CrawlStatus.STATUS_RETRY);
          }
        }

      } catch (final Throwable e) {
        LOG.error("fetcher throwable caught", e);
      } finally {
        if (fit != null) fetchQueues.finishFetchItem(fit);
        activeThreads.decrementAndGet(); // count threads
        LOG.info("-finishing thread " + getName() + ", activeThreads=" + activeThreads);
      }
    }

    private void handleRedirect(String url, String newUrl,
        boolean temp, String redirType, WebPage page)
    throws URLFilterException, IOException, InterruptedException {
      newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
      newUrl = urlFilters.filter(newUrl);
      if (newUrl == null || newUrl.equals(url)) {
        return;
      }

      if (ignoreExternalLinks) {
        String toHost   = new URL(newUrl).getHost().toLowerCase();
        String fromHost = new URL(url).getHost().toLowerCase();
        if (toHost == null || !toHost.equals(fromHost)) {
          // external links
          return;
        }
      }

      page.putToOutlinks(new Utf8(newUrl), new Utf8());
      page.putToMetadata(FetcherJob.REDIRECT_DISCOVERED, TableUtil.YES_VAL);
      reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temp);
      if (reprUrl == null) {
        LOG.warn("reprUrl==null");
      } else {
        page.setReprUrl(new Utf8(reprUrl));
        if (LOG.isDebugEnabled()) {
          LOG.debug(" - " + redirType + " redirect to " +
              reprUrl + " (fetching later)");
        }
      }
    }
    

    private void updateStatus(int bytesInPage) throws IOException {
      pages.incrementAndGet();
      bytes.addAndGet(bytesInPage);
    }

    private void output(FetchItem fit, Content content,
        ProtocolStatus pstatus, byte status)
    throws IOException, InterruptedException {
      fit.page.setStatus(status);
      final long prevFetchTime = fit.page.getFetchTime();
      fit.page.setPrevFetchTime(prevFetchTime);
      fit.page.setFetchTime(System.currentTimeMillis());
      if (pstatus != null) {
        fit.page.setProtocolStatus(pstatus);
      }

      if (content != null) {
        fit.page.setContent(ByteBuffer.wrap(content.getContent()));
        fit.page.setContentType(new Utf8(content.getContentType()));
        fit.page.setBaseUrl(new Utf8(content.getBaseUrl()));
      }
      Mark.FETCH_MARK.putMark(fit.page, Mark.GENERATE_MARK.checkMark(fit.page));
      String key = TableUtil.reverseUrl(fit.url);

      if (parse) {
        if (!skipTruncated || (skipTruncated && !ParserJob.isTruncated(fit.url, fit.page))) {
          parseUtil.process(key, fit.page);
        }
      }
      //remove content if storingContent is false. Content is added to fit.page above 
      //for ParseUtil be able to parse it. 
      if(content != null && !storingContent){
        fit.page.setContent(ByteBuffer.wrap(new byte[0]));
      }
      context.write(key, fit.page);
    }

    private void logFetchFailure(String url, String message) {
      LOG.warn("fetch of " + url + " failed with: " + message);
      errors.incrementAndGet();
    }
  }


  /**
   * This class feeds the queues with input items, and re-fills them as
   * items are consumed by FetcherThread-s.
   */
  private static class QueueFeeder extends Thread {
    private final Context context;
    private final FetchItemQueues queues;
    private final int size;
    private Iterator<FetchEntry> currentIter;
    boolean hasMore;
    private long timelimit = -1;

    public QueueFeeder(Context context,
        FetchItemQueues queues, int size)
    throws IOException, InterruptedException {
      this.context = context;
      this.queues = queues;
      this.size = size;
      this.setDaemon(true);
      this.setName("QueueFeeder");
      hasMore = context.nextKey();
      if (hasMore) {
        currentIter = context.getValues().iterator();
      }
      // the value of the time limit is either -1 or the time where it should finish
      timelimit = context.getConfiguration().getLong("fetcher.timelimit", -1); 
    }

    @Override
    public void run() {
      int cnt = 0;
      int timelimitcount = 0;
      try {
        while (hasMore) {
          if (System.currentTimeMillis() >= timelimit && timelimit != -1) {
            // enough .. lets' simply
            // read all the entries from the input without processing them
            while (currentIter.hasNext()) {
              currentIter.next();
              timelimitcount++;
            }
            hasMore = context.nextKey();
            if (hasMore) {
              currentIter = context.getValues().iterator();
            }
            continue;
          }
          int feed = size - queues.getTotalSize();
          if (feed <= 0) {
            // queues are full - spin-wait until they have some free space
            try {
              Thread.sleep(1000);
            } catch (final Exception e) {};
            continue;
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("-feeding " + feed + " input urls ...");
          }
          while (feed > 0 && currentIter.hasNext()) {
            FetchEntry entry = currentIter.next();
            final String url =
              TableUtil.unreverseUrl(entry.getKey());
            queues.addFetchItem(url, entry.getWebPage());
            feed--;
            cnt++;
          }
          if (currentIter.hasNext()) {
            continue; // finish items in current list before reading next key
          }
          hasMore = context.nextKey();
          if (hasMore) {
            currentIter = context.getValues().iterator();
          }
        }
      } catch (Exception e) {
        LOG.error("QueueFeeder error reading input, record " + cnt, e);
        return;
      }
      LOG.info("QueueFeeder finished: total " + cnt + " records. Hit by time limit :"
          + timelimitcount);
      context.getCounter("FetcherStatus","HitByTimeLimit-QueueFeeder").increment(timelimitcount);
    }
  }

  private void reportAndLogStatus(Context context, float actualPages, 
      int actualBytes, int totalSize) throws IOException {
    StringBuilder status = new StringBuilder();
    long elapsed = (System.currentTimeMillis() - start)/1000;
    status.append(spinWaiting).append("/").append(activeThreads).append(" spinwaiting/active, ");
    status.append(pages).append(" pages, ").append(errors).append(" errors, ");
    status.append(Math.round((((float)pages.get())*10)/elapsed)/10.0).append(" ");
    status.append(Math.round((actualPages*10)/10.0)).append(" pages/s, ");
    status.append(Math.round((((float)bytes.get())*8)/1024)/elapsed).append(" ");
    status.append(Math.round(((float)actualBytes)*8)/1024).append(" kb/s, ");
    status.append(totalSize).append(" URLs in ");
    status.append(this.fetchQueues.getQueueCount()).append(" queues");
    String toString = status.toString();
    context.setStatus(toString);
    LOG.info(toString);
  }

  @Override
  public void run(Context context)
  throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    this.fetchQueues = new FetchItemQueues(conf);
    int threadCount = conf.getInt("fetcher.threads.fetch", 10);
    parse = conf.getBoolean(FetcherJob.PARSE_KEY, false);
    storingContent=conf.getBoolean("fetcher.store.content", true);
    if (parse) {
      skipTruncated=conf.getBoolean(ParserJob.SKIP_TRUNCATED, true);
      parseUtil = new ParseUtil(conf);
    }
    LOG.info("Fetcher: threads: " + threadCount);

    int maxFeedPerThread = conf.getInt("fetcher.queue.depth.multiplier", 50);
    feeder = new QueueFeeder(context, fetchQueues, threadCount * maxFeedPerThread);
    feeder.start();

    for (int i = 0; i < threadCount; i++) {       // spawn threads
      FetcherThread ft = new FetcherThread(context, i);
      fetcherThreads.add(ft);
      ft.start();
    }
    // select a timeout that avoids a task timeout
    final long timeout = conf.getInt("mapred.task.timeout", 10*60*1000)/2;

    // Used for threshold check, holds pages and bytes processed in the last sec
    float pagesLastSec;
    int bytesLastSec;

    int throughputThresholdCurrentSequence = 0;

    int throughputThresholdPages = conf.getInt("fetcher.throughput.threshold.pages", -1);
    if (LOG.isInfoEnabled()) { LOG.info("Fetcher: throughput threshold: " + throughputThresholdPages); }
    int throughputThresholdSequence = conf.getInt("fetcher.throughput.threshold.sequence", 5);
    if (LOG.isInfoEnabled()) { 
      LOG.info("Fetcher: throughput threshold sequence: " + throughputThresholdSequence); 
    }
    long throughputThresholdTimeLimit = conf.getLong("fetcher.throughput.threshold.check.after", -1);
    
    do {                                          // wait for threads to exit
      pagesLastSec = pages.get();
      bytesLastSec = (int)bytes.get();
      final int secondsToSleep = 5;
      try {
        Thread.sleep(secondsToSleep * 1000);
      } catch (InterruptedException e) {}

      pagesLastSec = (pages.get() - pagesLastSec)/secondsToSleep;
      bytesLastSec = ((int)bytes.get() - bytesLastSec)/secondsToSleep;

      int fetchQueuesTotalSize = fetchQueues.getTotalSize();
      reportAndLogStatus(context, pagesLastSec, bytesLastSec, fetchQueuesTotalSize);
      
      boolean feederAlive = feeder.isAlive();
      if (!feederAlive && fetchQueuesTotalSize < 5) {
        fetchQueues.dump();
      }
      
      // check timelimit
      if (!feederAlive) {
        int hitByTimeLimit = fetchQueues.checkTimelimit();
        if (hitByTimeLimit != 0) {
          context.getCounter("FetcherStatus","HitByTimeLimit-Queues").increment(hitByTimeLimit);
        }
      }
      
      // if throughput threshold is enabled
      if (throughputThresholdTimeLimit < System.currentTimeMillis() && throughputThresholdPages != -1) {
        // Check if we're dropping below the threshold
        if (pagesLastSec < throughputThresholdPages) {
          throughputThresholdCurrentSequence++;
          LOG.warn(Integer.toString(throughputThresholdCurrentSequence) 
              + ": dropping below configured threshold of " + Integer.toString(throughputThresholdPages) 
              + " pages per second");

          // Quit if we dropped below threshold too many times
          if (throughputThresholdCurrentSequence > throughputThresholdSequence) {
            LOG.warn("Dropped below threshold too many times in a row, killing!");

            // Disable the threshold checker
            throughputThresholdPages = -1;

            // Empty the queues cleanly and get number of items that were dropped
            int hitByThrougputThreshold = fetchQueues.emptyQueues();

            if (hitByThrougputThreshold != 0) context.getCounter("FetcherStatus", 
                "hitByThrougputThreshold").increment(hitByThrougputThreshold);
          }
        } else {
          throughputThresholdCurrentSequence = 0;
        }
      }
      
      // some requests seem to hang, despite all intentions
      if ((System.currentTimeMillis() - lastRequestStart.get()) > timeout) {
        LOG.warn("Aborting with " + activeThreads + " hung threads.");
        return;
      }

    } while (activeThreads.get() > 0);
    LOG.info("-activeThreads=" + activeThreads);
  }
}

