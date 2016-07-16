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
import java.util.UUID;

import org.apache.nutch.webui.client.model.Crawl;
import org.apache.nutch.webui.client.model.JobInfo.JobType;
import org.joda.time.Duration;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

@Component
@Scope(BeanDefinition.SCOPE_PROTOTYPE)
public class RemoteCommandsBatchFactory {

  private List<RemoteCommand> remoteCommands;
  private Crawl crawl;

  private String batchId;

  public List<RemoteCommand> createCommands(Crawl crawl) {
    this.crawl = crawl;
    this.remoteCommands = Lists.newArrayList();

    remoteCommands.add(inject());
    for (int i = 0; i < crawl.getNumberOfRounds(); i++) {
      remoteCommands.addAll(createBatchCommands());
    }
    return remoteCommands;
  }

  private List<RemoteCommand> createBatchCommands() {
    this.batchId = UUID.randomUUID().toString();
    List<RemoteCommand> batchCommands = Lists.newArrayList();

    batchCommands.add(createGenerateCommand());
    batchCommands.add(createFetchCommand());
    batchCommands.add(createParseCommand());
    batchCommands.add(createUpdateDbCommand());
    batchCommands.add(createIndexCommand());

    return batchCommands;
  }

  private RemoteCommand inject() {
    RemoteCommandBuilder builder = RemoteCommandBuilder
        .instance(JobType.INJECT).withCrawlId(crawl.getCrawlId())
        .withArgument("url_dir", crawl.getSeedDirectory());
    return builder.build();
  }

  private RemoteCommand createGenerateCommand() {
    return createBuilder(JobType.GENERATE).build();
  }

  private RemoteCommand createFetchCommand() {
    return createBuilder(JobType.FETCH).withTimeout(
        Duration.standardSeconds(50)).build();
  }

  private RemoteCommand createParseCommand() {
    return createBuilder(JobType.PARSE).build();
  }

  private RemoteCommand createIndexCommand() {
    return createBuilder(JobType.INDEX).build();
  }

  private RemoteCommand createUpdateDbCommand() {
    return createBuilder(JobType.UPDATEDB).build();
  }

  private RemoteCommandBuilder createBuilder(JobType jobType) {
    return RemoteCommandBuilder.instance(jobType)
        .withCrawlId(crawl.getCrawlId()).withArgument("batch", batchId);
  }

}
