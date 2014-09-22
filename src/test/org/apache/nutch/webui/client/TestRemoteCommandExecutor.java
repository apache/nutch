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

import static org.junit.Assert.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.anyString;

import org.apache.nutch.webui.client.impl.RemoteCommand;
import org.apache.nutch.webui.client.impl.RemoteCommandBuilder;
import org.apache.nutch.webui.client.impl.RemoteCommandExecutor;
import org.apache.nutch.webui.client.model.JobInfo;
import org.apache.nutch.webui.client.model.JobInfo.JobType;
import org.apache.nutch.webui.client.model.JobInfo.State;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestRemoteCommandExecutor {
  private static final int REQUEST_DELAY = 10;

  @Mock
  private NutchClient client;

  @InjectMocks
  private RemoteCommandExecutor remoteExecutor = new RemoteCommandExecutor(client);

  @Before
  public void setUp() {
    remoteExecutor.setRequestDelay(Duration.millis(REQUEST_DELAY));
  }

  @Test
  public void shouldExecuteCommandRemotely() {
    // given
    RemoteCommand command = RemoteCommandBuilder.instance(JobType.INJECT).build();
    JobInfo jobInfo = new JobInfo();
    jobInfo.setState(State.FINISHED);
    given(client.getJobInfo(anyString())).willReturn(jobInfo);

    // when
    JobInfo info = remoteExecutor.executeRemoteJob(command);

    // then
    assertEquals(State.FINISHED, info.getState());
  }

  @Test
  public void shouldWaitUntilExecutionComplete() {
    // given
    RemoteCommand command = RemoteCommandBuilder.instance(JobType.INJECT)
        .withTimeout(Duration.standardSeconds(1)).build();
    JobInfo jobInfo = new JobInfo();
    jobInfo.setState(State.RUNNING);

    JobInfo newJobInfo = new JobInfo();
    newJobInfo.setState(State.FINISHED);
    given(client.getJobInfo(anyString())).willReturn(jobInfo, newJobInfo);

    // when
    JobInfo info = remoteExecutor.executeRemoteJob(command);

    // then
    assertEquals(State.FINISHED, info.getState());
  }

  @Test
  public void shouldThrowExceptionOnTimeout() {
    // given
    RemoteCommand command = RemoteCommandBuilder.instance(JobType.INJECT)
        .withTimeout(Duration.millis(REQUEST_DELAY / 2)).build();

    JobInfo jobInfo = new JobInfo();
    jobInfo.setState(State.RUNNING);

    given(client.getJobInfo(anyString())).willReturn(jobInfo);

    // when
    JobInfo info = remoteExecutor.executeRemoteJob(command);

    // then
    assertEquals(State.FAILED, info.getState());
  }
}
