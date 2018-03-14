/**
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
package org.apache.nutch.service.impl;

import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.nutch.service.model.response.JobInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;

public class NutchServerPoolExecutor extends ThreadPoolExecutor{

  private Queue<JobWorker> workersHistory;
  private Queue<JobWorker> runningWorkers;

  public NutchServerPoolExecutor(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue){
    super(corePoolSize, maxPoolSize, keepAliveTime, unit, workQueue);
    workersHistory = Queues.newArrayBlockingQueue(maxPoolSize);
    runningWorkers = Queues.newArrayBlockingQueue(maxPoolSize);
  }

  @Override
  protected void beforeExecute(Thread thread, Runnable runnable) {
    super.beforeExecute(thread, runnable);
    synchronized (runningWorkers) {
      runningWorkers.offer(((JobWorker) runnable));
    }
  }
  @Override
  protected void afterExecute(Runnable runnable, Throwable throwable) {
    super.afterExecute(runnable, throwable);
    synchronized (runningWorkers) {
      runningWorkers.remove(((JobWorker) runnable).getInfo());
    }
    JobWorker worker = ((JobWorker) runnable);
    addStatusToHistory(worker);
  }

  private void addStatusToHistory(JobWorker worker) {
    synchronized (workersHistory) {
      if (!workersHistory.offer(worker)) {
        workersHistory.poll();
        workersHistory.add(worker);
      }
    }
  }

  /**
   * Find the Job Worker Thread
   * @param jobId
   * @return
   */
  public JobWorker findWorker(String jobId) {
    synchronized (runningWorkers) {
      for (JobWorker worker : runningWorkers) {
        if (StringUtils.equals(worker.getInfo().getId(), jobId)) {
          return worker;
        }
      }
    }
    return null;
  }

  /**
   * Gives the Job history
   * @return
   */
  public Collection<JobInfo> getJobHistory() {
    return getJobsInfo(workersHistory);
  }

  /**
   * Gives the list of currently running jobs
   * @return
   */
  public Collection<JobInfo> getJobRunning() {
    return getJobsInfo(runningWorkers);
  }

  /**
   * Gives all jobs(currently running and completed)
   * @return
   */
  @SuppressWarnings("unchecked")
  public Collection<JobInfo> getAllJobs() {
    return CollectionUtils.union(getJobRunning(), getJobHistory());
  }

  private Collection<JobInfo> getJobsInfo(Collection<JobWorker> workers) {
    List<JobInfo> jobsInfo = Lists.newLinkedList();
    for (JobWorker worker : workers) {
      jobsInfo.add(worker.getInfo());
    }
    return jobsInfo;
  }


  public JobInfo getInfo(String jobId) {
    for (JobInfo jobInfo : getAllJobs()) {
      if (StringUtils.equals(jobId, jobInfo.getId())) {
        return jobInfo;
      }
    }
    return null;
  }

}
