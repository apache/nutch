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
package org.apache.nutch.webui.client;

import static org.apache.nutch.webui.client.model.JobInfo.JobType.FETCH;
import static org.apache.nutch.webui.client.model.JobInfo.JobType.GENERATE;
import static org.apache.nutch.webui.client.model.JobInfo.JobType.INDEX;
import static org.apache.nutch.webui.client.model.JobInfo.JobType.INJECT;
import static org.apache.nutch.webui.client.model.JobInfo.JobType.PARSE;
import static org.apache.nutch.webui.client.model.JobInfo.JobType.UPDATEDB;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.nutch.webui.client.impl.RemoteCommand;
import org.apache.nutch.webui.client.impl.RemoteCommandsBatchFactory;
import org.apache.nutch.webui.client.model.Crawl;
import org.apache.nutch.webui.client.model.JobInfo.JobType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class TestRemoteCommandsBatchFactory {
  private RemoteCommandsBatchFactory factory;

  private List<JobType> executionSequence = Lists.newArrayList(INJECT, GENERATE, FETCH, PARSE,
      UPDATEDB, INDEX, GENERATE, FETCH, PARSE, UPDATEDB, INDEX);

  @Before
  public void setUp() {
    factory = new RemoteCommandsBatchFactory();
  }

  @Test
  public void commandsShouldBeInCorrectSequence() {
    // given
    Crawl crawl = new Crawl();
    crawl.setNumberOfRounds(2);

    // when
    List<RemoteCommand> commands = factory.createCommands(crawl);

    // then
    for (int i = 0; i < commands.size(); i++) {
      assertTrue(commands.get(i).getJobConfig().getType() == executionSequence.get(i));
    }
  }

  @Test
  public void batchIdShouldBeUnique() {
    // given
    Crawl crawl = new Crawl();
    crawl.setNumberOfRounds(2);

    // when
    List<RemoteCommand> commands = factory.createCommands(crawl);

    // then
    String batchId = (String) commands.get(1).getJobConfig().getArgs().get("batch");
    String secondBatchId = (String) commands.get(7).getJobConfig().getArgs().get("batch");
    assertNotEquals(batchId, secondBatchId);
  }
}
