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

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.apache.nutch.webui.model.NutchInstance;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TestNutchClientFactory {
  
  @InjectMocks
  private NutchClientFactory factory;

  @Test
  public void shouldReturnClientWithCorrectInstance() {
    // given
    NutchInstance instance = new NutchInstance();

    // when
    NutchClient client = factory.getClient(instance);

    // then
    assertSame(instance, client.getNutchInstance());
  }

  @Test
  public void shouldReturnTheSameClient() {
    // given
    NutchInstance instance = new NutchInstance();

    // when
    NutchClient client = factory.getClient(instance);
    NutchClient client2 = factory.getClient(instance);

    // then
    assertSame(client, client2);
  }
  
  @Test
  public void shouldReturnNewClientForOtherInstance() {
    // given
    NutchInstance instance = new NutchInstance();
    NutchInstance otherInstance = new NutchInstance();

    // when
    NutchClient client = factory.getClient(instance);
    NutchClient client2 = factory.getClient(otherInstance);

    // then
    assertNotSame(client, client2);
  }
}
