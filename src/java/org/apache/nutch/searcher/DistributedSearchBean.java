package org.apache.nutch.searcher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.StringUtils;

public class DistributedSearchBean implements SearchBean {

  private static final ExecutorService executor =
    Executors.newCachedThreadPool();

  private final ScheduledExecutorService pingService;

  private class SearchTask implements Callable<Hits> {
    private int id;

    private Query query;
    private int numHits;
    private String dedupField;
    private String sortField;
    private boolean reverse;

    public SearchTask(int id) {
      this.id = id;
    }

    public Hits call() throws Exception {
      if (!liveServers[id]) {
        return null;
      }
      return beans[id].search(query, numHits, dedupField, sortField, reverse);
    }

    public void setSearchArgs(Query query, int numHits, String dedupField,
                              String sortField, boolean reverse) {
      this.query = query;
      this.numHits = numHits;
      this.dedupField = dedupField;
      this.sortField = sortField;
      this.reverse = reverse;
    }

  }

  private class DetailTask implements Callable<HitDetails[]> {
    private int id;

    private Hit[] hits;

    public DetailTask(int id) {
     this.id = id;
    }

    public HitDetails[] call() throws Exception {
      if (hits == null) {
        return null;
      }
      return beans[id].getDetails(hits);
    }

    public void setHits(Hit[] hits) {
      this.hits = hits;
    }

  }

  private class PingWorker implements Runnable {
    private int id;

    public PingWorker(int id) {
      this.id = id;
    }

    public void run()  {
      try {
        if (beans[id].ping()) {
          liveServers[id] = true;
        } else {
          liveServers[id] = false;
        }
      } catch (IOException e) {
        liveServers[id] = false;
      }
    }
  }

  private volatile boolean liveServers[];

  private SearchBean[] beans;

  private List<Callable<Hits>> searchTasks;

  private List<Callable<HitDetails[]>> detailTasks;

  private List<PingWorker> pingWorkers;

  private long timeout;

  public DistributedSearchBean(Configuration conf,
                               Path luceneConfig, Path solrConfig)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);

    this.timeout = conf.getLong("ipc.client.timeout", 60000);

    List<SearchBean> beanList = new ArrayList<SearchBean>();

    if (fs.exists(luceneConfig)) {
      addLuceneBeans(beanList, luceneConfig, conf);
    }

    if (fs.exists(solrConfig)) {
      addSolrBeans(beanList, solrConfig, conf);
    }

    beans = beanList.toArray(new SearchBean[beanList.size()]);

    liveServers = new boolean[beans.length];
    for (int i = 0; i < liveServers.length; i++) {
      liveServers[i] = true;
    }

    searchTasks = new ArrayList<Callable<Hits>>();
    detailTasks = new ArrayList<Callable<HitDetails[]>>();
    pingWorkers = new ArrayList<PingWorker>();

    for (int i = 0; i < beans.length; i++) {
      searchTasks.add(new SearchTask(i));
      detailTasks.add(new DetailTask(i));
      pingWorkers.add(new PingWorker(i));
    }

    pingService = Executors.newScheduledThreadPool(beans.length);
    for (PingWorker worker : pingWorkers) {
      pingService.scheduleAtFixedRate(worker, 0, 10, TimeUnit.SECONDS);
    }

  }

  private static void addLuceneBeans(List<SearchBean> beanList,
                                     Path luceneConfig, Configuration conf)
  throws IOException {
    Configuration newConf = new Configuration(conf);

    // do not retry connections
    newConf.setInt("ipc.client.connect.max.retries", 0);

    List<InetSocketAddress> luceneServers =
      NutchBean.readAddresses(luceneConfig, conf);
    for (InetSocketAddress addr : luceneServers) {
      beanList.add((RPCSearchBean) RPC.getProxy(RPCSearchBean.class,
          LuceneSearchBean.VERSION, addr, newConf));
    }
  }

  private static void addSolrBeans(List<SearchBean> beanList,
                                   Path solrConfig, Configuration conf)
  throws IOException {
    for (String solrServer : NutchBean.readConfig(solrConfig, conf)) {
      beanList.add(new SolrSearchBean(conf, solrServer));
    }
  }

  public String getExplanation(Query query, Hit hit) throws IOException {
    return beans[hit.getIndexNo()].getExplanation(query, hit);
  }

  public Hits search(Query query, int numHits, String dedupField,
                     String sortField, boolean reverse) throws IOException {

    for (Callable<Hits> task : searchTasks) {
      ((SearchTask)task).setSearchArgs(query, numHits, dedupField, sortField,
          reverse);
    }

    List<Future<Hits>> allHits;
    try {
      allHits =
        executor.invokeAll(searchTasks, timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    PriorityQueue<Hit> queue;            // cull top hits from results
    if (sortField == null || reverse) {
      queue = new PriorityQueue<Hit>(numHits);
    } else {
      queue = new PriorityQueue<Hit>(numHits, new Comparator<Hit>() {
        public int compare(Hit h1, Hit h2) {
          return h2.compareTo(h1); // reverse natural order
        }
      });
    }

    long totalHits = 0;
    int allHitsSize = allHits.size();
    for (int i = 0; i < allHitsSize; i++) {
      Hits hits = null;
      try {
        hits = allHits.get(i).get();
      } catch (InterruptedException e) {
        // ignore
      } catch (ExecutionException e) {
        LOG.warn("Retrieving hits failed with exception: " +
                 StringUtils.stringifyException(e.getCause()));
      }

      if (hits == null) {
        continue;
      }

      totalHits += hits.getTotal();

      int hitsLength = hits.getLength();
      for (int j = 0; j < hitsLength; j++) {
        Hit hit = hits.getHit(j);
        Hit newHit = new Hit(i, hit.getUniqueKey(),
                             hit.getSortValue(), hit.getDedupValue());
        queue.add(newHit);
        if (queue.size() > numHits) {         // if hit queue overfull
          queue.remove();
        }
      }
    }

    // we have to sort results since PriorityQueue.toArray
    // may not return results in sorted order
    Hit[] culledResults = queue.toArray(new Hit[queue.size()]);
    Arrays.sort(culledResults, Collections.reverseOrder(queue.comparator()));

    return new Hits(totalHits, culledResults);
  }

  public void close() throws IOException {
    executor.shutdown();
    pingService.shutdown();
    for (SearchBean bean : beans) {
      bean.close();
    }
  }

  public HitDetails getDetails(Hit hit) throws IOException {
    return beans[hit.getIndexNo()].getDetails(hit);
  }

  @SuppressWarnings("unchecked")
  public HitDetails[] getDetails(Hit[] hits) throws IOException {
    List<Hit>[] hitList = new ArrayList[detailTasks.size()];

    for (int i = 0; i < hitList.length; i++) {
      hitList[i] = new ArrayList<Hit>();
    }

    for (int i = 0; i < hits.length; i++) {
      Hit hit = hits[i];
      hitList[hit.getIndexNo()].add(hit);
    }

    for (int i = 0; i < detailTasks.size(); i++) {
      DetailTask task = (DetailTask)detailTasks.get(i);
      if (hitList[i].size() > 0) {
        task.setHits(hitList[i].toArray(new Hit[hitList[i].size()]));
      } else {
        task.setHits(null);
      }
    }

    List<Future<HitDetails[]>> allDetails;
    try {
      allDetails =
        executor.invokeAll(detailTasks, timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    /* getDetails(Hit[]) method assumes that HitDetails[i] returned corresponds
     * to Hit[i] given as parameter. To keep this order, we have to 'merge'
     * HitDetails[] returned from individual detailTasks.
     */
    HitDetails[][] detailsMatrix = new HitDetails[detailTasks.size()][];
    for (int i = 0; i < detailsMatrix.length; i++) {
      try {
        detailsMatrix[i] = allDetails.get(i).get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        throw new RuntimeException(e);
      }
    }

    int[] hitPos = new int[detailTasks.size()]; // keep track of where we are
    HitDetails[] detailsArr = new HitDetails[hits.length];
    for (int i = 0; i < detailsArr.length; i++) {
      int indexNo = hits[i].getIndexNo();
      detailsArr[i] = detailsMatrix[indexNo][(hitPos[indexNo]++)];
    }

    return detailsArr;
  }

  public boolean ping() {
    return true; // not used
  }

}
