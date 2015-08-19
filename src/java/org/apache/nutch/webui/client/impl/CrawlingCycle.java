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

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.nutch.webui.client.model.Crawl;
import org.apache.nutch.webui.client.model.JobInfo;
import org.apache.nutch.webui.client.model.JobInfo.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * This class implements crawl cycle as in crawl script
 * 
 * @author feodor
 * 
 */
public class CrawlingCycle {
  private Logger log = LoggerFactory.getLogger(CrawlingCycle.class);

  private CrawlingCycleListener listener;
  private RemoteCommandExecutor executor;
  private Crawl crawl;

  private List<RemoteCommand> remoteCommands;
  private List<RemoteCommand> executedCommands = Lists.newArrayList();

  public CrawlingCycle(CrawlingCycleListener listener,
      RemoteCommandExecutor executor, Crawl crawl, List<RemoteCommand> commands) {
    this.listener = listener;
    this.executor = executor;
    this.crawl = crawl;
    this.remoteCommands = commands;
  }

  public synchronized void executeCrawlCycle() {
    listener.crawlingStarted(crawl);

    for (RemoteCommand command : remoteCommands) {
      JobInfo jobInfo = executor.executeRemoteJob(command);
      command.setJobInfo(jobInfo);

      log.info("Executed remote command data: {}", command);

      if (jobInfo.getState() == State.FAILED) {
        listener.onCrawlError(crawl, jobInfo.getMsg());
        return;
      }

      executedCommands.add(command);
      listener.commandExecuted(crawl, command, calculateProgress());
    }
    listener.crawlingFinished(crawl);
  }

  private int calculateProgress() {
    if (CollectionUtils.isEmpty(remoteCommands)) {
      return 0;
    }
    return (int) ((float) executedCommands.size()
        / (float) remoteCommands.size() * 100);
  }

}
