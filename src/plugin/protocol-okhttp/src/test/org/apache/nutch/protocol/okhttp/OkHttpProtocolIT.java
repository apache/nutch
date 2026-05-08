/*
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
package org.apache.nutch.protocol.okhttp;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.AbstractProtocolPluginIT;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration tests for protocol-okhttp using a real nginx container.
 */
@Testcontainers(disabledWithoutDocker = true)
public class OkHttpProtocolIT extends AbstractProtocolPluginIT {

  @Container
  private static final GenericContainer<?> nginx =
      new GenericContainer<>("nginx:alpine").withExposedPorts(80);

  private OkHttp protocol;

  @Override
  public void setUpProtocol() throws Exception {
    Configuration conf = NutchConfiguration.create();
    conf.set("plugin.includes", "protocol-okhttp|lib-http|nutch-extensionpoints");
    conf.set("http.agent.name", "Nutch-Test");
    conf.setInt("http.timeout", 10000);
    conf.setBoolean("store.http.headers", true);
    protocol = new OkHttp();
    protocol.setConf(conf);
  }

  @Override
  public void tearDownProtocol() {
    protocol = null;
  }

  @Override
  public Protocol getProtocol() {
    return protocol;
  }

  @Override
  public String getTestUrl() {
    return "http://" + nginx.getHost() + ":" + nginx.getMappedPort(80) + "/";
  }

  /** OkHttp transparently decompresses gzip; verify content is returned. */
  @Test
  void testFetchWithAcceptEncoding() throws Exception {
    CrawlDatum datum = new CrawlDatum();
    ProtocolOutput output = protocol.getProtocolOutput(
        new org.apache.hadoop.io.Text(getTestUrl()), datum);
    assertNotNull(output.getContent(),
        "Content must be present even when server uses compression");
  }
}
