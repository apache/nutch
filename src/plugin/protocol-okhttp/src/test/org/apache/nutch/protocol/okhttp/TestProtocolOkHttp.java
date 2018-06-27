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

import static org.junit.Assert.assertEquals;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.okhttp.OkHttp;
import org.junit.After;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

/**
 * Test cases for protocol-http
 */
public class TestProtocolOkHttp {
  private static final String RES_DIR = System.getProperty("test.data", ".");

  private OkHttp http;
  private Server server;
  private Context root;
  private Configuration conf;
  private int port;

  public void setUp(boolean redirection) throws Exception {
    conf = new Configuration();
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site-test.xml");

    http = new OkHttp();
    http.setConf(conf);

    server = new Server();

    if (redirection) {
      root = new Context(server, "/redirection", Context.SESSIONS);
      root.setAttribute("newContextURL", "/redirect");
    } else {
      root = new Context(server, "/", Context.SESSIONS);
    }

    ServletHolder sh = new ServletHolder(
        org.apache.jasper.servlet.JspServlet.class);
    root.addServlet(sh, "*.jsp");
    root.setResourceBase(RES_DIR);
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test
  public void testStatusCode() throws Exception {
    startServer(47504, false);
    fetchPage("/basic-http.jsp", 200);
    fetchPage("/redirect301.jsp", 301);
    fetchPage("/redirect302.jsp", 302);
    fetchPage("/nonexists.html", 404);
    fetchPage("/brokenpage.jsp", 500);
  }

  @Test
  public void testRedirectionJetty() throws Exception {
    // Redirection via Jetty
    startServer(47503, true);
    fetchPage("/redirection", 302);
  }

  /**
   * Starts the Jetty server at a specified port and redirection parameter.
   * 
   * @param portno
   *          Port number.
   * @param redirection
   *          whether redirection
   */
  private void startServer(int portno, boolean redirection) throws Exception {
    port = portno;
    setUp(redirection);
    SelectChannelConnector connector = new SelectChannelConnector();
    connector.setHost("127.0.0.1");
    connector.setPort(port);

    server.addConnector(connector);
    server.start();
  }

  /**
   * Fetches the specified <code>page</code> from the local Jetty server and
   * checks whether the HTTP response status code matches with the expected
   * code. Also use jsp pages for redirection.
   * 
   * @param page
   *          Page to be fetched.
   * @param expectedCode
   *          HTTP response status code expected while fetching the page.
   */
  private void fetchPage(String page, int expectedCode) throws Exception {
    URL url = new URL("http", "127.0.0.1", port, page);
    CrawlDatum crawlDatum = new CrawlDatum();
    Response response = http.getResponse(url, crawlDatum, true);
    ProtocolOutput out = http.getProtocolOutput(new Text(url.toString()),
        crawlDatum);
    Content content = out.getContent();
    assertEquals("HTTP Status Code for " + url, expectedCode,
        response.getCode());

    if (page.compareTo("/nonexists.html") != 0
        && page.compareTo("/brokenpage.jsp") != 0
        && page.compareTo("/redirection") != 0) {
      assertEquals("ContentType " + url, "text/html",
          content.getContentType());
    }
  }
}
