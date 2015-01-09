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
package org.apache.nutch.webui.client.impl;

import java.io.Serializable;
import java.text.MessageFormat;

import org.apache.commons.lang3.StringUtils;
import org.apache.nutch.webui.client.model.JobConfig;
import org.apache.nutch.webui.client.model.JobInfo;
import org.joda.time.Duration;

public class RemoteCommand implements Serializable {
  private JobConfig jobConfig;
  private JobInfo jobInfo = new JobInfo();
  private Duration timeout;

  /**
   * Use {@link RemoteCommandBuilder} instead
   */
  @SuppressWarnings("unused")
  private RemoteCommand() {
  }

  public RemoteCommand(JobConfig jobConfig) {
    this.jobConfig = jobConfig;
  }

  public JobConfig getJobConfig() {
    return jobConfig;
  }

  public void setJobConfig(JobConfig jobConfig) {
    this.jobConfig = jobConfig;
  }

  public JobInfo getJobInfo() {
    return jobInfo;
  }

  public void setJobInfo(JobInfo jobInfo) {
    this.jobInfo = jobInfo;
  }

  public Duration getTimeout() {
    return timeout;
  }

  public void setTimeout(Duration timeout) {
    this.timeout = timeout;
  }

  @Override
  public String toString() {
    String statusInfo = StringUtils.EMPTY;
    if (jobInfo != null) {
      statusInfo = MessageFormat.format("{0}", jobInfo.getState());
    }
    return MessageFormat.format("{0} status: {1}", jobConfig.getType(),
        statusInfo);
  }
}
