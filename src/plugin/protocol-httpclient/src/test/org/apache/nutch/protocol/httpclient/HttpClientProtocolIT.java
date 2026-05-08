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
package org.apache.nutch.protocol.httpclient;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolPluginIntegrationTest;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for protocol-httpclient using an in-process WireMock
 * server.
 *
 * <p>WireMock runs in the test JVM so no Docker container is required. The
 * Nutch httpclient plugin connects to it over a real TCP socket, exercising
 * the full HTTP client stack including header handling and Basic-auth
 * challenge/response.
 */
public class HttpClientProtocolIT implements ProtocolPluginIntegrationTest {

  private static WireMockServer wireMock;
  private Http protocol;

  @BeforeAll
  static void startWireMock() {
    wireMock = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    wireMock.start();

    wireMock.stubFor(get(urlEqualTo("/"))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "text/html")
            .withBody("<html><body>Integration test</body></html>")));

    wireMock.stubFor(get(urlEqualTo("/notfound"))
        .willReturn(aResponse().withStatus(404)));

    wireMock.stubFor(get(urlEqualTo("/secure"))
        .withBasicAuth("testuser", "testpass")
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "text/html")
            .withBody("<html><body>Authenticated</body></html>")));

    wireMock.stubFor(get(urlEqualTo("/secure"))
        .willReturn(aResponse()
            .withStatus(401)
            .withHeader("WWW-Authenticate", "Basic realm=\"Test\"")
            .withBody("Unauthorized")));
  }

  @AfterAll
  static void stopWireMock() {
    if (wireMock != null) {
      wireMock.stop();
    }
  }

  @BeforeEach
  @Override
  public void setUpProtocol() throws Exception {
    Configuration conf = NutchConfiguration.create();
    conf.set("plugin.includes",
        "protocol-httpclient|lib-http|nutch-extensionpoints");
    conf.set("http.agent.name", "Nutch-Test");
    conf.setInt("http.timeout", 10000);
    conf.setBoolean("store.http.headers", true);
    protocol = new Http();
    protocol.setConf(conf);
  }

  @AfterEach
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
    return "http://localhost:" + wireMock.port() + "/";
  }

  @Test
  void testFetch200() throws Exception {
    CrawlDatum datum = new CrawlDatum();
    ProtocolOutput output = protocol.getProtocolOutput(new Text(getTestUrl()), datum);
    assertNotNull(output, "ProtocolOutput must not be null");
    int code = Integer.parseInt(
        datum.getMetaData().get(Nutch.PROTOCOL_STATUS_CODE_KEY).toString());
    assertEquals(200, code, "Expected HTTP 200 from WireMock stub");
  }

  @Test
  void testFetch404() throws Exception {
    String url = "http://localhost:" + wireMock.port() + "/notfound";
    CrawlDatum datum = new CrawlDatum();
    protocol.getProtocolOutput(new Text(url), datum);
    int code = Integer.parseInt(
        datum.getMetaData().get(Nutch.PROTOCOL_STATUS_CODE_KEY).toString());
    assertEquals(404, code, "Expected HTTP 404 for /notfound stub");
  }

  @Test
  void testUnauthenticatedRequestReturns401() throws Exception {
    String secureUrl = "http://localhost:" + wireMock.port() + "/secure";
    CrawlDatum datum = new CrawlDatum();
    protocol.getProtocolOutput(new Text(secureUrl), datum);
    int code = Integer.parseInt(
        datum.getMetaData().get(Nutch.PROTOCOL_STATUS_CODE_KEY).toString());
    assertEquals(401, code,
        "Unauthenticated request to /secure should return 401");
  }
}
