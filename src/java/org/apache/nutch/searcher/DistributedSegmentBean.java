package org.apache.nutch.searcher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;

public class DistributedSegmentBean implements SegmentBean {

  private static final ExecutorService executor =
    Executors.newCachedThreadPool();

  private final ScheduledExecutorService pingService;

  private class DistSummmaryTask implements Callable<Summary[]> {
    private int id;

    private HitDetails[] details;
    private Query query;

    public DistSummmaryTask(int id) {
      this.id = id;
    }

    public Summary[] call() throws Exception {
      if (details == null) {
        return null;
      }
      return beans[id].getSummary(details, query);
    }

    public void setSummaryArgs(HitDetails[] details, Query query) {
      this.details = details;
      this.query = query;
    }

  }

  private class SegmentWorker implements Runnable {
    private int id;

    public SegmentWorker(int id) {
      this.id = id;
    }

    public void run()  {
      try {
        String[] segments = beans[id].getSegmentNames();
        for (String segment : segments) {
          segmentMap.put(segment, id);
        }
      } catch (IOException e) {
        // remove all segments this bean was serving
        Iterator<Map.Entry<String, Integer>> i =
          segmentMap.entrySet().iterator();
        while (i.hasNext()) {
          Map.Entry<String, Integer> entry = i.next();
          int curId = entry.getValue();
          if (curId == this.id) {
            i.remove();
          }
        }
      }
    }
  }

  private long timeout;

  private SegmentBean[] beans;

  private ConcurrentMap<String, Integer> segmentMap;

  private List<Callable<Summary[]>> summaryTasks;

  private List<SegmentWorker> segmentWorkers;

  public DistributedSegmentBean(Configuration conf, Path serversConfig)
  throws IOException {
    this.timeout = conf.getLong("ipc.client.timeout", 60000);

    List<SegmentBean> beanList = new ArrayList<SegmentBean>();

    List<InetSocketAddress> segmentServers =
        NutchBean.readAddresses(serversConfig, conf);

    for (InetSocketAddress addr : segmentServers) {
      SegmentBean bean = (RPCSegmentBean) RPC.getProxy(RPCSegmentBean.class,
          FetchedSegments.VERSION, addr, conf);
      beanList.add(bean);
    }

    beans = beanList.toArray(new SegmentBean[beanList.size()]);

    summaryTasks = new ArrayList<Callable<Summary[]>>(beans.length);
    segmentWorkers = new ArrayList<SegmentWorker>(beans.length);

    for (int i = 0; i < beans.length; i++) {
      summaryTasks.add(new DistSummmaryTask(i));
      segmentWorkers.add(new SegmentWorker(i));
    }

    segmentMap = new ConcurrentHashMap<String, Integer>();

    pingService = Executors.newScheduledThreadPool(beans.length);
    for (SegmentWorker worker : segmentWorkers) {
      pingService.scheduleAtFixedRate(worker, 0, 30, TimeUnit.SECONDS);
    }
  }

  private SegmentBean getBean(HitDetails details) {
    return beans[segmentMap.get(details.getValue("segment"))];
  }

  public String[] getSegmentNames() {
    return segmentMap.keySet().toArray(new String[segmentMap.size()]);
  }

  public byte[] getContent(HitDetails details) throws IOException {
    return getBean(details).getContent(details);
  }

  public long getFetchDate(HitDetails details) throws IOException {
    return getBean(details).getFetchDate(details);
  }

  public ParseData getParseData(HitDetails details) throws IOException {
    return getBean(details).getParseData(details);
  }

  public ParseText getParseText(HitDetails details) throws IOException {
    return getBean(details).getParseText(details);
  }

  public void close() throws IOException {
    executor.shutdown();
    pingService.shutdown();
    for (SegmentBean bean : beans) {
      bean.close();
    }
  }

  public Summary getSummary(HitDetails details, Query query)
  throws IOException {
    return getBean(details).getSummary(details, query);
  }

  @SuppressWarnings("unchecked")
  public Summary[] getSummary(HitDetails[] detailsArr, Query query)
  throws IOException {
    List<HitDetails>[] detailsList = new ArrayList[summaryTasks.size()];
    for (int i = 0; i < detailsList.length; i++) {
      detailsList[i] = new ArrayList<HitDetails>();
    }
    for (HitDetails details : detailsArr) {
      detailsList[segmentMap.get(details.getValue("segment"))].add(details);
    }
    for (int i = 0; i < summaryTasks.size(); i++) {
      DistSummmaryTask task = (DistSummmaryTask)summaryTasks.get(i);
      if (detailsList[i].size() > 0) {
        HitDetails[] taskDetails =
          detailsList[i].toArray(new HitDetails[detailsList[i].size()]);
        task.setSummaryArgs(taskDetails, query);
      } else {
        task.setSummaryArgs(null, null);
      }
    }

    List<Future<Summary[]>> summaries;
    try {
       summaries =
         executor.invokeAll(summaryTasks, timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    List<Summary> summaryList = new ArrayList<Summary>();
    for (Future<Summary[]> f : summaries) {
      Summary[] summaryArray;
      try {
         summaryArray = f.get();
         if (summaryArray == null) {
           continue;
         }
         for (Summary summary : summaryArray) {
           summaryList.add(summary);
         }
      } catch (Exception e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        throw new RuntimeException(e);
      }
    }

    return summaryList.toArray(new Summary[summaryList.size()]);
  }

}
