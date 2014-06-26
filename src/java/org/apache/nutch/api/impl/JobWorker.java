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

import java.text.MessageFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.api.model.request.JobConfig;
import org.apache.nutch.api.model.response.JobInfo;
import org.apache.nutch.api.model.response.JobInfo.State;
import org.apache.nutch.api.resources.ConfigResource;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.NutchTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobWorker implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(JobWorker.class);
  private NutchTool tool;
  private JobInfo jobInfo;
  private JobConfig jobConfig;

  public JobWorker(JobConfig jobConfig, Configuration conf, NutchTool tool) {
    this.tool = tool;
    this.jobConfig = jobConfig;
    if (jobConfig.getConfId() == null) {
      jobConfig.setConfId(ConfigResource.DEFAULT);
    }

    jobInfo = new JobInfo(generateId(), jobConfig, State.IDLE, "idle");
    if (jobConfig.getCrawlId() != null) {
      conf.set(Nutch.CRAWL_ID_KEY, jobConfig.getCrawlId());
    }
  }

  private String generateId() {
    if (jobConfig.getCrawlId() == null) {
      return MessageFormat.format("{0}-{1}-{2}", jobConfig.getConfId(),
          jobConfig.getType(), String.valueOf(hashCode()));
    }
    return MessageFormat.format("{0}-{1}-{2}-{3}", jobConfig.getCrawlId(),
        jobConfig.getConfId(), jobConfig.getType(), String.valueOf(hashCode()));
  }

  @Override
  public void run() {
    try {
      getInfo().setState(State.RUNNING);
      getInfo().setMsg("OK");
      getInfo().setResult(tool.run(getInfo().getArgs()));
      getInfo().setState(State.FINISHED);
    } catch (Exception e) {
      LOG.error("Cannot run job worker!", e);
      getInfo().setMsg("ERROR: " + e.toString());
      getInfo().setState(State.FAILED);
    }
  }

  public boolean stopJob() {
    getInfo().setState(State.STOPPING);
    try {
      return tool.stopJob();
    } catch (Exception e) {
      throw new RuntimeException(
          "Cannot stop job with id " + getInfo().getId(), e);
    }
  }

  public boolean killJob() {
    getInfo().setState(State.KILLING);
    try {
      boolean result = tool.killJob();
      getInfo().setState(State.KILLED);
      return result;
    } catch (Exception e) {
      throw new RuntimeException(
          "Cannot kill job with id " + getInfo().getId(), e);
    }
  }

  public JobInfo getInfo() {
    return jobInfo;
  }

  public void setInfo(JobInfo jobInfo) {
    this.jobInfo = jobInfo;
  }

}