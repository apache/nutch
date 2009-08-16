package org.apache.nutch.fetcher;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
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

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.nutch.crawl.CrawlDatumHbase;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.protocol.RobotRules;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.URLUtil;
import org.apache.nutch.util.hbase.WebTableColumns;
import org.apache.nutch.util.hbase.TableUtil;
import org.apache.nutch.util.hbase.WebTableRow;

public class FetcherReducer
extends TableReducer<ImmutableBytesWritable, FetchEntry, ImmutableBytesWritable> {

  public static final Log LOG = Fetcher.LOG;

  private final AtomicInteger activeThreads = new AtomicInteger(0);
  private final AtomicInteger spinWaiting = new AtomicInteger(0);

  private final long start = System.currentTimeMillis(); // start time of fetcher run
  private final AtomicLong lastRequestStart = new AtomicLong(start);

  private final AtomicLong bytes = new AtomicLong(0);        // total bytes fetched
  private final AtomicInteger pages = new AtomicInteger(0);  // total pages fetched
  private final AtomicInteger errors = new AtomicInteger(0); // total pages errored

  private QueueFeeder feeder;

  private List<FetcherThread> fetcherThreads = new ArrayList<FetcherThread>();

  private FetchItemQueues fetchQueues;
  
  private static final ImmutableBytesWritable REDUCE_KEY =
    new ImmutableBytesWritable(TableUtil.YES_VAL);

  /**
   * This class described the item to be fetched.
   */
  private static class FetchItem {
    WebTableRow row;
    String queueID;
    String url;
    URL u;

    public FetchItem(String url, WebTableRow row, URL u, String queueID) {
      this.row = row;
      this.url = url;
      this.u = u;
      this.queueID = queueID;
    }

    /** Create an item. Queue id will be created based on <code>byIP</code>
     * argument, either as a protocol + hostname pair, or protocol + IP
     * address pair.
     */
    public static FetchItem create(String url, WebTableRow row, boolean byIP) {
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
      if (byIP) {
        try {
          final InetAddress addr = InetAddress.getByName(u.getHost());
          host = addr.getHostAddress();
        } catch (final UnknownHostException e) {
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
      return new FetchItem(url, row, u, queueID);
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

    public void addFetchItem(String url, WebTableRow row) {
      final FetchItem it = FetchItem.create(url, row, byIP);
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
        // initialize queue
        fiq = new FetchItemQueue(conf, maxThreads, crawlDelay, minCrawlDelay);
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

    public synchronized void dump() {
      for (final String id : queues.keySet()) {
        final FetchItemQueue fiq = queues.get(id);
        if (fiq.getQueueSize() == 0) continue;
        LOG.info("* queue: " + id);
        fiq.dump();
      }
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
    private final int maxRedirect;
    private String reprUrl;
    private boolean redirecting;
    private int redirectCount;
    private Context context;

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
      this.maxRedirect = conf.getInt("http.redirect.max", 3);
    }

    @Override
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
          if (!fit.row.hasColumn(WebTableColumns.REPR_URL, null)) {
            reprUrl = fit.url.toString();
          } else {
            reprUrl = fit.row.getReprUrl();
          }
          try {
            LOG.info("fetching " + fit.url);

            // fetch the page
            redirecting = false;
            redirectCount = 0;
            do {
              if (LOG.isDebugEnabled()) {
                LOG.debug("redirectCount=" + redirectCount);
              }
              redirecting = false;
              final Protocol protocol = this.protocolFactory.getProtocol(fit.url);
              final RobotRules rules = protocol.getRobotRules(fit.url, fit.row);
              if (!rules.isAllowed(fit.u)) {
                // unblock
                fetchQueues.finishFetchItem(fit, true);
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Denied by robots.txt: " + fit.url);
                }
                output(fit, null, ProtocolStatus.STATUS_ROBOTS_DENIED,
                    CrawlDatumHbase.STATUS_GONE);
                continue;
              }
              if (rules.getCrawlDelay() > 0) {
                if (rules.getCrawlDelay() > maxCrawlDelay) {
                  // unblock
                  fetchQueues.finishFetchItem(fit, true);
                  LOG.debug("Crawl-Delay for " + fit.url + " too long (" + rules.getCrawlDelay() + "), skipping");
                  output(fit, null, ProtocolStatus.STATUS_ROBOTS_DENIED, CrawlDatumHbase.STATUS_GONE);
                  continue;
                } else {
                  final FetchItemQueue fiq = fetchQueues.getFetchItemQueue(fit.queueID);
                  fiq.crawlDelay = rules.getCrawlDelay();
                }
              }
              final ProtocolOutput output = protocol.getProtocolOutput(fit.url, fit.row);
              final ProtocolStatus status = output.getStatus();
              final Content content = output.getContent();
              // unblock queue
              fetchQueues.finishFetchItem(fit);

              switch(status.getCode()) {

              case ProtocolStatus.WOULDBLOCK:
                // retry ?
                fetchQueues.addFetchItem(fit);
                break;

              case ProtocolStatus.SUCCESS:        // got a page
                output(fit, content, status, CrawlDatumHbase.STATUS_FETCHED);
                updateStatus(content.getContent().length);
                break;

              case ProtocolStatus.MOVED:         // redirect
              case ProtocolStatus.TEMP_MOVED:
                byte code;
                boolean temp;
                if (status.getCode() == ProtocolStatus.MOVED) {
                  code = CrawlDatumHbase.STATUS_REDIR_PERM;
                  temp = false;
                } else {
                  code = CrawlDatumHbase.STATUS_REDIR_TEMP;
                  temp = true;
                }
                output(fit, content, status, code);
                final String newUrl = status.getMessage();
                handleRedirect(fit.url, newUrl, temp,  Fetcher.PROTOCOL_REDIR);
                redirecting = false;
                break;
              case ProtocolStatus.EXCEPTION:
                logError(fit.url, status.getMessage());
                /* FALLTHROUGH */
              case ProtocolStatus.RETRY:          // retry
              case ProtocolStatus.BLOCKED:
                output(fit, null, status, CrawlDatumHbase.STATUS_RETRY);
                break;

              case ProtocolStatus.GONE:           // gone
              case ProtocolStatus.NOTFOUND:
              case ProtocolStatus.ACCESS_DENIED:
              case ProtocolStatus.ROBOTS_DENIED:
                output(fit, null, status, CrawlDatumHbase.STATUS_GONE);
                break;

              case ProtocolStatus.NOTMODIFIED:
                output(fit, null, status, CrawlDatumHbase.STATUS_NOTMODIFIED);
                break;

              default:
                if (LOG.isWarnEnabled()) {
                  LOG.warn("Unknown ProtocolStatus: " + status.getCode());
                }
                output(fit, null, status, CrawlDatumHbase.STATUS_RETRY);
              }

              if (redirecting && redirectCount >= maxRedirect) {
                fetchQueues.finishFetchItem(fit);
                if (LOG.isInfoEnabled()) {
                  LOG.info(" - redirect count exceeded " + fit.url);
                }
                output(fit, null, ProtocolStatus.STATUS_REDIR_EXCEEDED, CrawlDatumHbase.STATUS_GONE);
              }

            } while (redirecting && (redirectCount < maxRedirect));

          } catch (final Throwable t) {                 // unexpected exception
            // unblock
            fetchQueues.finishFetchItem(fit);
            t.printStackTrace();
            logError(fit.url, t.toString());
            output(fit, null, ProtocolStatus.STATUS_FAILED, CrawlDatumHbase.STATUS_RETRY);
          }
        }

      } catch (final Throwable e) {
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

    private void handleRedirect(String url, String newUrl,
        boolean temp, String redirType)
    throws URLFilterException, IOException, InterruptedException {
      newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
      newUrl = urlFilters.filter(newUrl);
      if (newUrl == null || newUrl.equals(url)) {
        return;
      }
      reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temp);
      final String reversedNewUrl = TableUtil.reverseUrl(newUrl);
      // TODO: Find a way to use MutablWebTableRow here
      Put put = new Put(Bytes.toBytes(reversedNewUrl));
      if (!reprUrl.equals(url)) {
        put.add(WebTableColumns.REPR_URL, null, Bytes.toBytes(reprUrl));
      }
      put.add(WebTableColumns.METADATA,
              Fetcher.REDIRECT_DISCOVERED, TableUtil.YES_VAL);
      context.write(REDUCE_KEY, put);
      if (LOG.isDebugEnabled()) {
        LOG.debug(" - " + redirType + " redirect to " +
            reprUrl + " (fetching later)");
      }

    }

    private void updateStatus(int bytesInPage) throws IOException {
      pages.incrementAndGet();
      bytes.addAndGet(bytesInPage);
    }

    private void output(FetchItem fit, Content content,
        ProtocolStatus pstatus, byte status)
    throws IOException, InterruptedException {
      fit.row.setStatus(status);
      final long prevFetchTime = fit.row.getFetchTime();
      fit.row.setPrevFetchTime(prevFetchTime);
      fit.row.setFetchTime(System.currentTimeMillis());
      if (pstatus != null) {
        fit.row.setProtocolStatus(pstatus);
      }

      if (content != null) {
        fit.row.setContent(content.getContent());
        fit.row.setContentType(content.getContentType());
        fit.row.setBaseUrl(content.getBaseUrl());
      }
      fit.row.putMeta(Fetcher.FETCH_MARK, TableUtil.YES_VAL);
      fit.row.makeRowMutation().writeToContext(REDUCE_KEY, context);
    }

    private void logError(String url, String message) {
      if (LOG.isInfoEnabled()) {
        LOG.info("fetch of " + url + " failed with: " + message);
      }
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
    }

    @Override
    public void run() {
      int cnt = 0;
      try {
        while (hasMore) {
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
            FetchEntry entry = new FetchEntry();
            // since currentIter.next() reuses the same
            // FetchEntry object we need to clone it
            Writables.copyWritable(currentIter.next(), entry);
            WebTableRow row = new WebTableRow(entry.getRow());
            final String url =
              TableUtil.unreverseUrl(Bytes.toString(entry.getKey().get()));
            queues.addFetchItem(url, row);
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
        LOG.fatal("QueueFeeder error reading input, record " + cnt, e);
        return;
      }
      LOG.info("QueueFeeder finished: total " + cnt + " records.");
    }
  }

  @Override
  public void run(Context context)
  throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    this.fetchQueues = new FetchItemQueues(conf);
    final int threadCount = conf.getInt("fetcher.threads.fetch", 10);
    LOG.info("FetcherHbase: threads: " + threadCount);

    // set non-blocking & no-robots mode for HTTP protocol plugins.
    conf.setBoolean(Protocol.CHECK_BLOCKING, false);
    conf.setBoolean(Protocol.CHECK_ROBOTS, false);

    feeder = new QueueFeeder(context, fetchQueues, threadCount * 50);
    feeder.start();

    for (int i = 0; i < threadCount; i++) {       // spawn threads
      FetcherThread ft = new FetcherThread(context, i);
      fetcherThreads.add(ft);
      ft.start();
    }
    // select a timeout that avoids a task timeout
    final long timeout = conf.getInt("mapred.task.timeout", 10*60*1000)/2;

    do {                                          // wait for threads to exit
      try {
        Thread.sleep(10000);
      } catch (final InterruptedException e) {}

      context.progress();
      LOG.info("-activeThreads=" + activeThreads + ", spinWaiting=" + spinWaiting.get()
          + ", fetchQueues= " + fetchQueues.getQueueCount() +", fetchQueues.totalSize=" + fetchQueues.getTotalSize());

      if (!feeder.isAlive() && fetchQueues.getTotalSize() < 5) {
        fetchQueues.dump();
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
