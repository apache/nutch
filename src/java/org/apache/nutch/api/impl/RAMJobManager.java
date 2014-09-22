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

import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.api.ConfManager;
import org.apache.nutch.api.JobManager;
import org.apache.nutch.api.model.request.JobConfig;
import org.apache.nutch.api.model.response.JobInfo;
import org.apache.nutch.api.model.response.JobInfo.State;
import org.apache.nutch.util.NutchTool;

public class RAMJobManager implements JobManager {
  private JobFactory jobFactory;
  private NutchServerPoolExecutor executor;
  private ConfManager configManager;

  public RAMJobManager(JobFactory jobFactory, NutchServerPoolExecutor executor,
      ConfManager configManager) {
    this.jobFactory = jobFactory;
    this.executor = executor;
    this.configManager = configManager;
  }

  @Override
  public Collection<JobInfo> list(String crawlId, State state) {
    if (state == null || state == State.ANY) {
      return executor.getAllJobs();
    }
    if (state == State.RUNNING || state == State.IDLE) {
      return executor.getJobRunning();
    }
    return executor.getJobHistory();
  }

  @Override
  public JobInfo get(String crawlId, String jobId) {
    return executor.getInfo(jobId);
  }

  @Override
  public String create(JobConfig jobConfig) {
    if (jobConfig.getArgs() == null) {
      throw new IllegalArgumentException("Arguments cannot be null!");
    }

    Configuration conf = cloneConfiguration(jobConfig.getConfId());
    NutchTool tool = createTool(jobConfig, conf);
    JobWorker worker = new JobWorker(jobConfig, conf, tool);

    executor.execute(worker);
    executor.purge();
    return worker.getInfo().getId();
  }

  private Configuration cloneConfiguration(String confId) {
    Configuration conf = configManager.get(confId);
    if (conf == null) {
      throw new IllegalArgumentException("Unknown confId " + confId);
    }
    return new Configuration(conf);
  }

  private NutchTool createTool(JobConfig jobConfig, Configuration conf) {
    if (StringUtils.isNotBlank(jobConfig.getJobClassName())) {
      return jobFactory.createToolByClassName(jobConfig.getJobClassName(), conf);
    }
    
    return jobFactory.createToolByType(jobConfig.getType(), conf);
  }

  @Override
  public boolean abort(String crawlId, String id) {
    return executor.findWorker(id).killJob();
  }

  @Override
  public boolean stop(String crawlId, String id) {
    return executor.findWorker(id).stopJob();
  }
}
