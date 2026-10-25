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
package org.apache.nutch.protocol.selenium;

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
 * Integration tests for protocol-selenium using a real nginx container.
 *
 * <p>Note: protocol-selenium uses raw HTTP sockets (the same underlying
 * transport as protocol-http) rather than a Selenium WebDriver. Tests here
 * validate that the plugin connects to and fetches content from a live HTTP
 * server. Browser-based rendering is covered by protocol-interactiveselenium
 * which is excluded from automated integration tests due to its stateful
 * handler requirements.
 */
@Testcontainers(disabledWithoutDocker = true)
public class SeleniumProtocolIT extends AbstractProtocolPluginIT {

  @Container
  private static final GenericContainer<?> nginx =
      new GenericContainer<>("nginx:alpine").withExposedPorts(80);

  private Http protocol;

  @Override
  public void setUpProtocol() throws Exception {
    Configuration conf = NutchConfiguration.create();
    conf.set("plugin.includes",
        "protocol-selenium|lib-http|lib-selenium|nutch-extensionpoints");
    conf.set("http.agent.name", "Nutch-Test");
    conf.setInt("http.timeout", 10000);
    conf.setBoolean("store.http.headers", true);
    protocol = new Http();
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

  @Test
  void testFetchReturnsContent() throws Exception {
    CrawlDatum datum = new CrawlDatum();
    ProtocolOutput output = protocol.getProtocolOutput(
        new org.apache.hadoop.io.Text(getTestUrl()), datum);
    assertNotNull(output.getContent(),
        "protocol-selenium must return non-null content for a live nginx page");
  }
}
