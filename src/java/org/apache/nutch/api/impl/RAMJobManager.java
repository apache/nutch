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
package org.apache.nutch.api.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.nutch.api.ConfResource;
import org.apache.nutch.api.JobManager;
import org.apache.nutch.api.JobStatus;
import org.apache.nutch.api.JobStatus.State;
import org.apache.nutch.api.NutchApp;
import org.apache.nutch.crawl.Crawler;
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
    typeToClass.put(JobType.CRAWL, Crawler.class);
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
  @SuppressWarnings("fallthrough")
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
  public JobStatus get(String crawlId, String jobId) throws Exception {
    for (JobStatus job : jobRunning) {
      if (job.id.equals(jobId)) {
        return job;
      }
    }
    for (JobStatus job : jobHistory) {
      if (job.id.equals(jobId)) {
        return job;
      }
    }
    return null;
  }

  @Override
  public String create(String crawlId, JobType type, String confId,
      Map<String,Object> args) throws Exception {
    if (args == null) args = Collections.emptyMap();
    JobWorker worker = new JobWorker(crawlId, type, confId, args);
    String id = worker.getId();
    exec.execute(worker);
    exec.purge();
    return id;
  }

  @Override
  public boolean abort(String crawlId, String id) throws Exception {
    // find running job
    for (JobStatus job : jobRunning) {
      if (job.id.equals(id)) {
        job.state = State.KILLING;
        boolean res = job.tool.killJob();
        job.state = State.KILLED;
        return res;
      }
    }
    return false;
  }

  @Override
  public boolean stop(String crawlId, String id) throws Exception {
    // find running job
    for (JobStatus job : jobRunning) {
      if (job.id.equals(id)) {
        job.state = State.STOPPING;
        boolean res = job.tool.stopJob();
        return res;
      }
    }
    return false;
  }
  
  private class JobWorker implements Runnable {
    String id;
    JobType type;
    String confId;
    NutchTool tool;
    Map<String,Object> args;
    JobStatus jobStatus;
    
    @SuppressWarnings("unchecked")
    JobWorker(String crawlId, JobType type, String confId, Map<String,Object> args) throws Exception {
      if (confId == null) {
        confId = ConfResource.DEFAULT_CONF;
      }
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
        Class<?> c = Class.forName((String)args.get(Nutch.ARG_CLASS));
        if(c instanceof Class) {
          clz = (Class<? extends NutchTool>) c;
        }
      }
      tool = ReflectionUtils.newInstance(clz, conf);
      jobStatus = new JobStatus(id, type, confId, args, State.IDLE, "idle");
      jobStatus.tool = tool;
    }
    
    public String getId() {
      return id;
    }
    
    public float getProgress() {
      return tool.getProgress();
    }
    
    public State getState() {
      return jobStatus.state;
    }
    
    public Map<String,Object> getResult() {
      return jobStatus.result;
    }
    
    public Map<String,Object> getStatus() {
      return tool.getStatus();
    }

    @Override
    public void run() {
      try {
        jobStatus.state = State.RUNNING;
        jobStatus.msg = "OK";
        jobStatus.result = tool.run(args);
        jobStatus.state = State.FINISHED;
      } catch (Exception e) {
        e.printStackTrace();
        jobStatus.msg = "ERROR: " + e.toString();
        jobStatus.state = State.FAILED;
      }
    }
  }
}
