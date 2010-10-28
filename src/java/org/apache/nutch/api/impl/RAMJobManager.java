package org.apache.nutch.api.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.nutch.api.JobManager;
import org.apache.nutch.api.JobStatus;
import org.apache.nutch.api.JobStatus.State;
import org.apache.nutch.api.NutchApp;
import org.apache.nutch.crawl.DbUpdaterJob;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.InjectorJob;
import org.apache.nutch.crawl.WebTableReader;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.indexer.solr.SolrIndexerJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.util.NutchTool;

public class RAMJobManager implements JobManager {
  int CAPACITY = 100;
  ThreadPoolExecutor exec = new MyPoolExecutor(10, CAPACITY, 1, TimeUnit.HOURS,
      new ArrayBlockingQueue<Runnable>(CAPACITY));
  
  private class MyPoolExecutor extends ThreadPoolExecutor {

    public MyPoolExecutor(int corePoolSize, int maximumPoolSize,
        long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
      super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
      // TODO Auto-generated method stub
      super.beforeExecute(t, r);
      synchronized(jobRunning) {
        jobRunning.offer(((JobWorker)r).jobStatus);
      }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
      super.afterExecute(r, t);
      synchronized(jobRunning) {
        jobRunning.remove(((JobWorker)r).jobStatus);
      }
      JobStatus status = ((JobWorker)r).jobStatus;
      synchronized(jobHistory) {
        if (!jobHistory.offer(status)) {
          jobHistory.poll();
          jobHistory.add(status);
        }
      }
    }
  }
  
  ArrayBlockingQueue<JobStatus> jobHistory = new ArrayBlockingQueue<JobStatus>(CAPACITY);
  ArrayBlockingQueue<JobStatus> jobRunning = new ArrayBlockingQueue<JobStatus>(CAPACITY);
  
  private static Map<JobType,Class<? extends NutchTool>> typeToClass = new HashMap<JobType,Class<? extends NutchTool>>();
  
  static {
    typeToClass.put(JobType.FETCH, FetcherJob.class);
    typeToClass.put(JobType.GENERATE, GeneratorJob.class);
    typeToClass.put(JobType.INDEX, SolrIndexerJob.class);
    typeToClass.put(JobType.INJECT, InjectorJob.class);
    typeToClass.put(JobType.PARSE, ParserJob.class);
    typeToClass.put(JobType.UPDATEDB, DbUpdaterJob.class);
    typeToClass.put(JobType.READDB, WebTableReader.class);
  }

  private void addFinishedStatus(JobStatus status) {
    synchronized(jobHistory) {
      if (!jobHistory.offer(status)) {
        jobHistory.poll();
        jobHistory.add(status);
      }
    }
  }
  
  @Override
  public List<JobStatus> list(String crawlId, State state) throws Exception {
    List<JobStatus> res = new ArrayList<JobStatus>();
    if (state == null) state = State.ANY;
    switch(state) {
    case ANY:
      res.addAll(jobHistory);
      /* FALLTHROUGH */
    case RUNNING:
    case IDLE:
      res.addAll(jobRunning);
      break;
    default:
      res.addAll(jobHistory);
    }
    return res;
  }

  @Override
  public Map<String, String> get(String crawlId, String jobId) throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String create(String crawlId, JobType type, String confId, Object... args) throws Exception {
    JobWorker worker = new JobWorker(crawlId, type, confId, args);
    String id = worker.getId();
    exec.execute(worker);
    exec.purge();
    return id;
  }

  @Override
  public boolean abort(String crawlId, String id) throws Exception {
    return false;
  }

  @Override
  public boolean stop(String crawlId, String id) throws Exception {
    // TODO Auto-generated method stub
    return false;
  }
  
  private class JobWorker implements Runnable {
    String id;
    JobType type;
    String confId;
    NutchTool tool;
    Object[] args;
    float progress = 0f;
    Job currentJob = null;
    JobStatus jobStatus;
    
    JobWorker(String crawlId, JobType type, String confId, Object... args) throws Exception {
      Configuration conf = NutchApp.confMgr.get(confId);
      // clone it - we are going to modify it
      if (conf == null) {
        throw new Exception("Unknown confId " + confId);
      }
      this.id = confId + "-" + type + "-" + hashCode();
      this.type = type;
      this.confId = confId;
      this.args = args;
      conf = new Configuration(conf);
      if (crawlId != null) {
        conf.set(Nutch.CRAWL_ID_KEY, crawlId);
        this.id = crawlId + "-" + this.id;
      }
      Class<? extends NutchTool> clz = typeToClass.get(type);
      if (clz == null) {
        clz = (Class<? extends NutchTool>)args[0];
      }
      tool = ReflectionUtils.newInstance(clz, conf);
      jobStatus = new JobStatus(id, type, confId, args, State.IDLE, "idle");
    }
    
    public String getId() {
      return id;
    }
    
    public float getProgress() {
      return progress;
    }
    
    public State getState() {
      return jobStatus.state;
    }
    
    private final float[] noProgress = new float[2];
    
    public float[] getCurrentJobProgress() throws IOException {
      if (currentJob == null) {
        return noProgress;
      }
      float[] res = new float[2];
      res[0] = currentJob.mapProgress();
      res[1] = currentJob.reduceProgress();
      return res;
    }
    
    public Map<String,Object> getResult() {
      return jobStatus.result;
    }
    
    public String getStatus() {
      return jobStatus.msg;
    }

    @Override
    public void run() {
      try {
        progress = 0f;
        jobStatus.state = State.RUNNING;
        jobStatus.msg = "prepare";
        tool.prepare();
        progress = 0.1f;
        Job[] jobs = tool.createJobs(args);
        float delta = 0.8f / jobs.length;
        for (int i = 0; i < jobs.length; i++) {
          currentJob = jobs[i];
          jobStatus.msg = "job " + (i + 1) + "/" + jobs.length;
          boolean success = jobs[i].waitForCompletion(true);
          if (!success) {
            throw new Exception("Job failed.");
          }
          jobStatus.msg = "postJob " + (i + 1);
          tool.postJob(i, jobs[i]);
          progress += delta;
        }
        currentJob = null;
        progress = 0.9f;
        jobStatus.msg = "finish";
        Map<String,Object> res = tool.finish();
        if (res != null) {
          jobStatus.result = res;
        }
        progress = 1.0f;
        jobStatus.state = State.FINISHED;
      } catch (Exception e) {
        jobStatus.msg = "ERROR " + jobStatus.msg + ": " + e.toString();
        jobStatus.state = State.FAILED;
      }
    }
  }
}
