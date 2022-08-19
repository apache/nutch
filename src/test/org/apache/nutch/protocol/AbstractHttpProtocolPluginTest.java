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
package org.apache.nutch.protocol;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.Response;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class providing methods to easily implement unit tests for HTTP
 * protocol plugins.
 */
public abstract class AbstractHttpProtocolPluginTest {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  protected Protocol http;
  protected ServerSocket server;
  protected Configuration conf;

  /** Protocol / URL scheme used to send/receive test requests */
  protected String protocol = "http";

  /**
   * URL host name used to represent localhost when sending/receiving test
   * requests
   */
  protected String localHost = "127.0.0.1";

  /** Port used to send/receive test requests */
  protected int defaultPort = 47505;

  protected static final String responseHeader = "HTTP/1.1 200 OK\r\n";
  protected static final String simpleContent = "Content-Type: text/html\r\n\r\nThis is a text.";
  protected static final String notFound = "HTTP/1.1 404 Not Found\r\n"
      + "Content-Type: text/html\r\n\r\n" //
      + "<html>\n<head><title>404 Not Found</title></head>\n" //
      + "<body>\n<h1>404 Not Found</h1>\n</body>\n</html>";
  protected static final String redirect301 = "HTTP/1.1 301 Moved Permanently\r\n" //
      + "Content-Type: text/html; charset=UTF-8\r\n" //
      + "Content-Length: 0\r\n" //
      + "Location: https://nutch.apache.org/\r\n\r\n";
  protected static final String redirect302 = "HTTP/1.1 302 Found\r\n" //
      + "Content-Type: text/html; charset=UTF-8\r\n" //
      + "Content-Length: 0\r\n" //
      + "Location: https://nutch.apache.org/\r\n\r\n";
  protected static final String serverError = "HTTP/1.1 500 Internal Server Error\r\n" //
      + "Server: Nutch Test\r\n" //
      + "Content-Length: 21\r\n" //
      + "Content-Type: text/html\r\n\r\n" //
      + "Internal Server Error";
  protected static final String badRequest = "HTTP/1.1 400 Bad request\r\n\r\n";

  protected abstract String getPluginClassName();

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

    http = new ProtocolFactory(conf)
        .getProtocolById(getPluginClassName());
    http.setConf(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (server != null) {
      server.close();
    }
  }

  /**
   * Starts the test server at a specified port and constant response.
   * 
   * @param portno
   *          Port number.
   * @param responder
   *          function to return a response (byte[] containing HTTP response
   *          header and payload content) for a given request header represented
   *          as list of request header lines
   * @param requestChecker
   *          verify request passed as list of HTTP header lines
   * @throws Exception
   */
  protected void runServer(int port,
      BiFunction<String, String[], byte[]> responder,
      Predicate<List<String>> requestChecker) throws Exception {
    server = new ServerSocket();
    server.bind(new InetSocketAddress(localHost, port));
    Pattern requestPattern = Pattern.compile("(?i)^GET\\s+(\\S+)");
    while (true) {
      LOG.info("Listening on port {}", port);
      if (server.isClosed()) {
        server = new ServerSocket();
        server.bind(new InetSocketAddress(localHost, port));
      }
      Socket socket = server.accept();
      LOG.info("Connection received");
      try (BufferedReader in = new BufferedReader(new InputStreamReader(
          socket.getInputStream(), UTF_8))) {

        List<String> requestLines = new ArrayList<>();
        String line;
        while ((line = in.readLine()) != null) {
          LOG.info("Request: {}", line);
          if (line.trim().isEmpty()) {
            break;
          }
          requestLines.add(line);
        }
        String requestPath = null;
        Matcher m = requestPattern.matcher(requestLines.get(0));
        if (m.find()) {
          requestPath = m.group(1);
          LOG.info("Requested path {}", requestPath);
        }
        byte[] response = badRequest.getBytes(UTF_8);
        if (requestChecker != null && !requestChecker.test(requestLines)) {
          LOG.warn("Request validation failed!");
          response = "HTTP/1.1 500 Internal Server Error\r\n\r\nRequest validation failed!"
              .getBytes(UTF_8);
        } else if (requestPath == null) {
          LOG.warn("No request path!");
         // bad request
        } else if (!requestPath.startsWith("/")) {
          // bad request
          LOG.warn("Request path must start with `/`");
        } else {
          response = responder.apply(requestPath,
              requestLines.toArray(new String[requestLines.size()]));
          if (response == null) {
            LOG.warn("No response found for given path `{}`", requestPath);
            response = notFound.getBytes(UTF_8);
          }
        }
        socket.getOutputStream().write(response);
      } catch (Exception e) {
        LOG.error("Exception in test server:", e);
      }
    }
  }

  protected void launchServer(int port, BiFunction<String, String[], byte[]> responder,
      Predicate<List<String>> requestChecker) throws InterruptedException {
    Thread serverThread = new Thread(() -> {
      try {
        runServer(port, responder, requestChecker);
      } catch (SocketException e) {
        LOG.info("Socket on port {} closed: {}", port, e.getMessage());
      } catch (Exception e) {
        LOG.warn("Test server died:", e);
      }
    });
    serverThread.start();
    Thread.sleep(50);
  }

  protected void launchServer(Function<String, byte[]> responder)
      throws InterruptedException {
    launchServer(responder, null);
  }

  protected void launchServer(Function<String, byte[]> responder,
      Predicate<List<String>> requestChecker) throws InterruptedException {
    launchServer(defaultPort, responder, requestChecker);
  }

  protected void launchServer(int port, Function<String, byte[]> responder,
      Predicate<List<String>> requestChecker) throws InterruptedException {
    BiFunction<String, String[], byte[]> responderBiFunc = (String path, String[] ignoredHeaders) -> {
      return responder.apply(path);
    };
    launchServer(port, responderBiFunc, requestChecker);
  }

  protected void launchServer(Map<String, byte[]> responses)
      throws InterruptedException {
    launchServer(defaultPort, (String requestPath) -> {
      return responses.get(requestPath);
    }, null);
  }

  protected void launchServer(String response) throws InterruptedException {
    launchServer("/", response);
  }

  protected void launchServer(String path, String response)
      throws InterruptedException {
    launchServer(path, response.getBytes(UTF_8));
  }

  protected void launchServer(String path, byte[] response)
      throws InterruptedException {
    Map<String, byte[]> responses = new TreeMap<>();
    responses.put(path, response);
    launchServer(responses);
  }

  protected ProtocolOutput fetchPage(String page, int expectedCode)
      throws Exception {
    return fetchPage(defaultPort, page, expectedCode, null);
  }

  protected ProtocolOutput fetchPage(String page, int expectedCode,
      String expectedContentType) throws Exception {
    return fetchPage(defaultPort, page, expectedCode, null);
  }

  /**
   * Fetches the specified <code>page</code> from the local test server and
   * checks whether the HTTP response status code matches with the expected
   * code.
   * 
   * @param port
   *          port server is running on
   * @param page
   *          Page to be fetched
   * @param expectedCode
   *          HTTP response status code expected while fetching the page
   * @param expectedContentType
   *          Expected Content-Type
   */
  protected ProtocolOutput fetchPage(int port, String page, int expectedCode,
      String expectedContentType) throws Exception {
    URL url = new URL(protocol, localHost, port, page);
    LOG.info("Fetching {}", url);
    CrawlDatum crawlDatum = new CrawlDatum();
    ProtocolOutput protocolOutput = http
        .getProtocolOutput(new Text(url.toString()), crawlDatum);
    int httpStatusCode = -1;
    if (crawlDatum.getMetaData().containsKey(Nutch.PROTOCOL_STATUS_CODE_KEY)) {
      httpStatusCode = Integer.parseInt(crawlDatum.getMetaData()
          .get(Nutch.PROTOCOL_STATUS_CODE_KEY).toString());
    }
    assertEquals("HTTP Status Code for " + url, expectedCode, httpStatusCode);
    if (httpStatusCode == 200 && expectedContentType != null) {
      Content content = protocolOutput.getContent();
      assertEquals("ContentType " + url, "text/html", content.getContentType());
    }
    return protocolOutput;
  }

  public static String getHeaders(ProtocolOutput response) {
    return response.getContent().getMetadata().get(Response.RESPONSE_HEADERS);
  }

  public static String getHeader(ProtocolOutput response, String header) {
    return getHeader(getHeaders(response).split("\r\n"), header);
  }

  public static String getHeader(String[] headers, String header) {
    for (String line : headers) {
      String[] parts = line.split(": ", 2);
      if (parts[0].equals(header)) {
        return parts[1];
      }
    }
    return null;
  }

}
