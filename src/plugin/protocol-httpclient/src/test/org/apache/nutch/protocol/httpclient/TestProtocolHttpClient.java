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

import java.net.URL;
import java.net.MalformedURLException;
import junit.framework.TestCase;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHttpContext;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.http.SocketListener;
import org.mortbay.http.handler.ResourceHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;

/**
 * Test cases for protocol-httpclient.
 *
 * @author Susam Pal
 */
public class TestProtocolHttpClient extends TestCase {

  private Server server;
  private Configuration conf;
  private static final String RES_DIR = System.getProperty("test.data", ".");
  private int port;
  private Http http = new Http();

  protected void setUp() throws Exception {

    ServletHttpContext context = new ServletHttpContext();
    context.setContextPath("/");
    context.setResourceBase(RES_DIR);
    context.addServlet("JSP", "*.jsp", "org.apache.jasper.servlet.JspServlet");
    context.addHandler(new ResourceHandler());

    server = new Server();
    server.addContext(context);

    conf = new Configuration();
    conf.addResource("nutch-default.xml");
    conf.addResource("nutch-site-test.xml");
    
    http = new Http();
    http.setConf(conf);
  }

  protected void tearDown() throws Exception {
    server.stop();
  }

  /**
   * Tests whether the client can remember cookies.
   *
   * @throws Exception If an error occurs or the test case fails.
   */
  public void testCookies() throws Exception {
    startServer(47500);
    fetchPage("/cookies.jsp", 200);
    fetchPage("/cookies.jsp?cookie=yes", 200);
  }

  /**
   * Tests that no pre-emptive authorization headers are sent by the
   * client.
   *
   * @throws Exception If an error occurs or the test case fails.
   */
  public void testNoPreemptiveAuth() throws Exception {
    startServer(47500);
    fetchPage("/noauth.jsp", 200);
  }

  /**
   * Tests default credentials.
   *
   * @throws Exception If an error occurs or the test case fails.
   */
  public void testDefaultCredentials() throws Exception {
    startServer(47502);
    fetchPage("/basic.jsp", 200);
  }

  /**
   * Tests basic authentication scheme for various realms.
   * 
   * @throws Exception If an error occurs or the test case fails.
   */
  public void testBasicAuth() throws Exception {
    startServer(47500);
    fetchPage("/basic.jsp", 200);
    fetchPage("/basic.jsp?case=1", 200);
    fetchPage("/basic.jsp?case=2", 200);
    server.start();
  }

  /**
   * Tests that authentication happens for a defined realm and not for
   * other realms for a host:port when an extra <code>authscope</code>
   * tag is not defined to match all other realms.
   *
   * @throws Exception If an error occurs or the test case fails.
   */
  public void testOtherRealmsNoAuth() throws Exception {
    startServer(47501);
    fetchPage("/basic.jsp", 200);
    fetchPage("/basic.jsp?case=1", 401);
    fetchPage("/basic.jsp?case=2", 401);
  }

  /**
   * Tests Digest authentication scheme.
   *
   * @throws Exception If an error occurs or the test case fails.
   */
  public void testDigestAuth() throws Exception {
    startServer(47500);
    fetchPage("/digest.jsp", 200);
  }

  /**
   * Tests NTLM authentication scheme.
   *
   * @throws Exception If an error occurs or the test case fails.
   */
  public void testNtlmAuth() throws Exception {
    startServer(47501);
    fetchPage("/ntlm.jsp", 200);
  }

  /**
   * Starts the Jetty server at a specified port.
   *
   * @param  portno     Port number.
   * @throws Exception  When an error occurs.
   */
  private void startServer(int portno) throws Exception {
    port = portno;
    SocketListener listener = new SocketListener();
    listener.setHost("127.0.0.1");
    listener.setPort(port);
    server.addListener(listener);
    server.start();
  }

  /**
   * Fetches the specified <code>page</code> from the local Jetty server
   * and checks whether the HTTP response status code matches with the
   * expected code.
   *
   * @param page          Page to be fetched.
   * @param expectedCode  HTTP response status code expected while
   *                      fetching the page.
   * @throws Exception    When an error occurs or test case fails.
   */
  private void fetchPage(String page, int expectedCode)
      throws Exception {
    URL url = new URL("http", "127.0.0.1", port, page);
    Response response = null;
    response = http.getResponse(url, new CrawlDatum(), true);

    int code = response.getCode();
    assertEquals("HTTP Status Code for " + url, expectedCode, code);
  }
  
  /**
   * Returns an URL to the specified page.
   *
   * @param  page                  Page available in the local Jetty
   *                               server.
   * @throws MalformedURLException If an URL can not be formed.
   */
  private URL getURL(String page) throws MalformedURLException {
    return new URL("http", "127.0.0.1", port, page);
  }
}
