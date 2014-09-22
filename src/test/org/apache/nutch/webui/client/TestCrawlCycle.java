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

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.nutch.webui.client.model.JobInfo.JobType.GENERATE;
import static org.apache.nutch.webui.client.model.JobInfo.JobType.INJECT;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.List;

import org.apache.nutch.webui.client.impl.CrawlingCycle;
import org.apache.nutch.webui.client.impl.CrawlingCycleListener;
import org.apache.nutch.webui.client.impl.RemoteCommand;
import org.apache.nutch.webui.client.impl.RemoteCommandBuilder;
import org.apache.nutch.webui.client.impl.RemoteCommandExecutor;
import org.apache.nutch.webui.client.model.Crawl;
import org.apache.nutch.webui.client.model.JobInfo;
import org.apache.nutch.webui.client.model.JobInfo.State;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestCrawlCycle {
  @Mock
  private RemoteCommandExecutor executor;

  @Mock
  private CrawlingCycleListener listener;

  private CrawlingCycle crawlingCycle;

  @Captor
  private ArgumentCaptor<RemoteCommand> remoteCommandCaptor;

  @Before
  public void setUp() {
    JobInfo jobInfo = new JobInfo();
    jobInfo.setState(State.FINISHED);
    given(executor.executeRemoteJob(any(RemoteCommand.class))).willReturn(jobInfo);
  }

  @Test
  public void shouldInvokeCrawlStartedAndFinished() {
    // given
    List<RemoteCommand> commands = newArrayList(RemoteCommandBuilder.instance(INJECT).build());
    Crawl crawl = new Crawl();

    crawlingCycle = new CrawlingCycle(listener, executor, crawl, commands);

    // when
    crawlingCycle.executeCrawlCycle();

    // then
    verify(listener, times(1)).crawlingStarted(crawl);
    verify(listener, times(1)).crawlingFinished(crawl);
  }

  @Test
  public void shouldInvokeOnError() {
    // given
    List<RemoteCommand> commands = newArrayList(RemoteCommandBuilder.instance(INJECT).build());
    Crawl crawl = new Crawl();
    crawlingCycle = new CrawlingCycle(listener, executor, crawl, commands);
    JobInfo jobInfo = new JobInfo();
    jobInfo.setMsg("Some error message");
    jobInfo.setState(State.FAILED);

    given(executor.executeRemoteJob(any(RemoteCommand.class))).willReturn(jobInfo);

    // when
    crawlingCycle.executeCrawlCycle();

    // then
    verify(listener, times(1)).onCrawlError(crawl, jobInfo.getMsg());
  }

  @Test
  public void shouldCalculateProgress() {
    // given
    RemoteCommand firstCommand = RemoteCommandBuilder.instance(INJECT).build();
    RemoteCommand secondCommand = RemoteCommandBuilder.instance(GENERATE).build();
    List<RemoteCommand> commands = newArrayList(firstCommand, secondCommand);

    Crawl crawl = new Crawl();
    crawlingCycle = new CrawlingCycle(listener, executor, crawl, commands);

    // when
    crawlingCycle.executeCrawlCycle();

    // then
    verify(listener, times(1)).commandExecuted(crawl, firstCommand, 50);
    verify(listener, times(1)).commandExecuted(crawl, secondCommand, 100);
  }

}
