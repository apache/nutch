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

import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nutch.webui.client.NutchClient;
import org.apache.nutch.webui.client.model.JobInfo;
import org.apache.nutch.webui.client.model.JobInfo.State;
import org.joda.time.DateTimeConstants;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class executes remote job and waits for success/failure result
 * 
 * @author feodor
 * 
 */
public class RemoteCommandExecutor {
  private Logger log = LoggerFactory.getLogger(RemoteCommandExecutor.class);

  private static final int DEFAULT_TIMEOUT_SEC = 60;
  private Duration requestDelay = new Duration(500);

  private NutchClient client;
  private ExecutorService executor;

  public RemoteCommandExecutor(NutchClient client) {
    this.client = client;
    this.executor = Executors.newSingleThreadExecutor();
  }

  public JobInfo executeRemoteJob(RemoteCommand command) {
    try {
      String jobId = client.executeJob(command.getJobConfig());
      Future<JobInfo> chekerFuture = executor.submit(new JobStateChecker(jobId));
      return chekerFuture.get(getTimeout(command), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      log.error("Remote command failed", e);
      JobInfo jobInfo = new JobInfo();
      jobInfo.setState(State.FAILED);
      jobInfo.setMsg(ExceptionUtils.getStackTrace(e));
      return jobInfo;
    }
  }

  private long getTimeout(RemoteCommand command) {
    if (command.getTimeout() == null) {
      return DEFAULT_TIMEOUT_SEC * DateTimeConstants.MILLIS_PER_SECOND;
    }
    return command.getTimeout().getMillis();
  }

  public void setRequestDelay(Duration requestDelay) {
    this.requestDelay = requestDelay;
  }

  public class JobStateChecker implements Callable<JobInfo> {

    private String jobId;

    public JobStateChecker(String jobId) {
      this.jobId = jobId;
    }

    @Override
    public JobInfo call() throws Exception {
      while (!Thread.interrupted()) {
        JobInfo jobInfo = client.getJobInfo(jobId);
        checkState(jobInfo != null, "Cannot get job info!");

        State state = jobInfo.getState();
        checkState(state != null, "Unknown job state!");

        if (state == State.RUNNING || state == State.ANY || state == State.IDLE) {
          Thread.sleep(requestDelay.getMillis());
          continue;
        }

        return jobInfo;
      }
      return null;
    }

  }
}
