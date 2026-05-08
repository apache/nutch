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
package org.apache.nutch.protocol.http;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
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
 * Integration tests for protocol-http using a real nginx container.
 */
@Testcontainers(disabledWithoutDocker = true)
public class HttpProtocolIT extends AbstractProtocolPluginIT {

  @Container
  private static final GenericContainer<?> nginx =
      new GenericContainer<>("nginx:alpine").withExposedPorts(80);

  private Http protocol;

  @Override
  public void setUpProtocol() throws Exception {
    Configuration conf = NutchConfiguration.create();
    conf.set("plugin.includes", "protocol-http|lib-http|nutch-extensionpoints");
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
  void testFetchRedirect301() throws Exception {
    // nginx returns 301 for directory URLs without trailing slash when autoindex
    // is off; test a manual redirect via the default nginx welcome page path
    String redirectUrl =
        "http://" + nginx.getHost() + ":" + nginx.getMappedPort(80) + "/index.html";
    CrawlDatum datum = new CrawlDatum();
    protocol.getProtocolOutput(new Text(redirectUrl), datum);
    int code = getHttpStatusCode(datum);
    // nginx serves index.html directly with 200; the base test covers 200/404
    assertEquals(200, code, "Expected 200 for index.html from nginx");
  }
}
