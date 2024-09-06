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

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.TreeMap;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.AbstractHttpProtocolPluginTest;
import org.apache.nutch.protocol.ProtocolException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

public class TestResponse extends AbstractHttpProtocolPluginTest {

  protected static final String redirectHeader = "HTTP/1.1 301 Moved Permanently\r\n" //
      + "Content-Type: text/html; charset=UTF-8\r\n" //
      + "Content-Length: 0\r\n";

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  @Override
  protected String getPluginClassName() {
    return "org.apache.nutch.protocol.okhttp.OkHttp";
  }

  @Override
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.addResource("nutch-default.xml");
    /*
     * plugin tests specific config file - needs to add the tested plugin to
     * plugin.includes
     */
    conf.addResource("nutch-site-test.xml");
    conf.setBoolean("store.http.headers", true);

    http = new OkHttp();
    http.setConf(conf);
  }

  protected OkHttpResponse getResponse(int statusCode, String headerName) {
    try {
      URL url = new URL(protocol, localHost, serverPort, "/" + headerName);
      LOG.info("Emulating fetch of {}", url);
      return new OkHttpResponse((OkHttp) http, url, new CrawlDatum(statusCode, 1000));
    } catch (ProtocolException | IOException e) {
      return null;
    }
  }

  protected void headerTest(int statusCode, String headerName, String value, String lookupName) {
    OkHttpResponse response = getResponse(statusCode, headerName);
    LOG.info("Response headers:");
    LOG.info(response.getHeaders().get(Response.RESPONSE_HEADERS));
    assertEquals(
        "No or unexpected value of header \"" + headerName
            + "\" returned when retrieving header \"" + lookupName + "\"",
        value, response.getHeader(lookupName));
  }

  protected Map<String, byte[]> getResponses(String headerValue) {
    String[] headerNames = { "Location", "location", "LOCATION", "Loction" };
    Map<String, byte[]> responses = new TreeMap<>();
    for (String headerName : headerNames) {
      responses.put("/" + headerName,
          (redirectHeader + headerName + ": " + headerValue + "\r\n"
              + "Content-Length: 0\r\n\r\n").getBytes(UTF_8));
    }
    responses.put("/MyCustomHeader", (responseHeader + "MyCustomHeader" + ": "
        + headerValue + "\r\n" + simpleContent).getBytes(UTF_8));
    return responses;
  }

  @Test
  public void testGetHeader() throws Exception {
    String value = "headervalue";
    launchServer(getResponses(value));

    LOG.info(
        "Testing standard HTTP header \"Location\": expected case-insensitive and error-tolerant matching");
    headerTest(301, "Location", value, "Location");
    headerTest(301, "Location", value, "location");
    headerTest(301, "location", value, "Location");
    headerTest(301, "LOCATION", value, "Location");
    // only with SpellCheckedMetadata:
    // headerTest(301, "Loction", value, "Location");

    LOG.info(
        "Testing non-standard HTTP header \"MyCustomHeader\": only exact matching");
    headerTest(200, "MyCustomHeader", value, "MyCustomHeader");
    /*
     * The following case-insensitive or approximate look-ups are not supported
     * for non-standard headers by SpellCheckedMetadata:
     */
    // testHeader(200, "MyCustomHeader", value, "mycustomheader");
    // testHeader(200, "mycustomheader", value, "MyCustomHeader");
    // testHeader(200, "MYCUSTOMHEADER", value, "MyCustomHeader");
  }

  @Ignore("Only for benchmarking")
  @Test
  public void testMetadataBenchmark() throws MalformedURLException, ProtocolException,
      IOException, InterruptedException {
    String value = "headervalue";
    launchServer(getResponses(value));
    Thread.sleep(30000); // time to attach a profiler
    int iterations = 5000;
    LOG.info("Starting benchmark with {} iterations ({} calls)", iterations,
        (iterations * 4));
    long start = System.currentTimeMillis();
    for (int i = 0; i < iterations; i++) {
      headerTest(301, "Location", value, "Location");
      headerTest(301, "Location", value, "location");
      headerTest(301, "location", value, "Location");
      headerTest(301, "LOCATION", value, "Location");
      // only with SpellCheckedMetadata:
      // headerTest(301, "Loction", value, "Location");
    }
    long elapsed = System.currentTimeMillis() - start;
    LOG.info("Benchmark finished, elapsed: {}, {}ms per call", elapsed,
        (elapsed / (4.0 * iterations)));
  }

}
