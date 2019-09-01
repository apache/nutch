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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test cases for protocol-http - robustness regarding bad server responses:
 * malformed HTTP header lines, etc. See, NUTCH-2549.
 */
public class TestBadServerResponses {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Protocol http;
  private ServerSocket server;
  private Configuration conf;
  private int port = 47506;

  private static final String responseHeader = "HTTP/1.1 200 OK\r\n";
  private static final String simpleContent = "Content-Type: text/html\r\n\r\nThis is a text.";

  public void setUp() throws Exception {
    conf = NutchConfiguration.create();
    conf.addResource("nutch-default.xml");
    // plugin tests specific config file - adds protocol-okhttp to
    // plugin.includes
    conf.addResource("nutch-site-test.xml");
    conf.setBoolean("store.http.headers", true);

    http = new ProtocolFactory(conf)
        .getProtocolById("org.apache.nutch.protocol.okhttp.OkHttp");
  }

  @After
  public void tearDown() throws Exception {
    server.close();
  }

  public static String getHeaders(ProtocolOutput response) {
    return response.getContent().getMetadata().get(Response.RESPONSE_HEADERS);
  }

  public static String getHeader(ProtocolOutput response, String header) {
    for (String line : getHeaders(response).split("\r\n")) {
      String[] parts = line.split(": ", 1);
      if (parts[0].equals(header)) {
        return parts[1];
      }
    }
    return null;
  }

  /**
     * Starts the test server at a specified port and constant response.
     * 
     * @param portno
     *          Port number.
     * @param response
     *          response sent on every request
     */
  private void runServer(int port, byte[] response) throws Exception {
    server = new ServerSocket();
    server.bind(new InetSocketAddress("127.0.0.1", port));
    Pattern requestPattern = Pattern.compile("(?i)^GET\\s+(\\S+)");
    while (true) {
      LOG.info("Listening on port {}", port);
      Socket socket = server.accept();
      LOG.info("Connection received");
      try (
          BufferedReader in = new BufferedReader(new InputStreamReader(
              socket.getInputStream(), StandardCharsets.UTF_8))) {

        String line;
        while ((line = in.readLine()) != null) {
          LOG.info("Request: {}", line);
          if (line.trim().isEmpty()) {
            break;
          }
          Matcher m = requestPattern.matcher(line);
          if (m.find()) {
            LOG.info("Requested {}", m.group(1));
            if (!m.group(1).startsWith("/")) {
              response = "HTTP/1.1 400 Bad request\r\n\r\n".getBytes(StandardCharsets.UTF_8);
            }
          }
        }
        socket.getOutputStream().write(response);
      } catch (Exception e) {
        LOG.warn("Exception in test server:", e);
      }
    }
  }

  private void launchServer(String response) throws InterruptedException {
    launchServer(response.getBytes(StandardCharsets.UTF_8));
  }

  private void launchServer(byte[] response) throws InterruptedException {
    Thread serverThread = new Thread(() -> {
      try {
        runServer(port, response);
      } catch (Exception e) {
        LOG.warn("Test server died:", e);
      }
    });
    serverThread.start();
    Thread.sleep(50);
  }

  /**
   * Fetches the specified <code>page</code> from the local test server and
   * checks whether the HTTP response status code matches with the expected
   * code.
   * 
   * @param page
   *          Page to be fetched.
   * @param expectedCode
   *          HTTP response status code expected while fetching the page.
   */
  private ProtocolOutput fetchPage(String page, int expectedCode)
      throws MalformedURLException {
    URL url = new URL("http", "127.0.0.1", port, page);
    LOG.info("Fetching {}", url);
    CrawlDatum crawlDatum = new CrawlDatum();
    ProtocolOutput out = http.getProtocolOutput(new Text(url.toString()),
        crawlDatum);
    int httpStatusCode = -1;
    if (crawlDatum.getMetaData().containsKey(Nutch.PROTOCOL_STATUS_CODE_KEY)) {
      httpStatusCode = Integer.parseInt(crawlDatum.getMetaData()
          .get(Nutch.PROTOCOL_STATUS_CODE_KEY).toString());
    }

    assertEquals("HTTP Status Code for " + url, expectedCode, httpStatusCode);

    return out;
  }

  @Test
  public void testBadHttpServer() throws Exception {
    setUp();
    // test with trivial well-formed content, to make sure the server is
    // responding
    launchServer(responseHeader + simpleContent);
    fetchPage("/", 200);
  }

  /**
   * NUTCH-2555 URL normalization problem: path not starting with a '/'
   */
  @Test
  public void testRequestNotStartingWithSlash() throws Exception {
    setUp();
    launchServer(responseHeader + simpleContent);
    fetchPage("?171", 200);
  }

  /**
   * NUTCH-2564 protocol-http throws an error when the content-length header is
   * not a number
   */
  @Test
  public void testContentLengthNotANumber() throws Exception {
    setUp();
    launchServer(
        responseHeader + "Content-Length: thousand\r\n" + simpleContent);
    fetchPage("/", 200);
  }

  /**
   * NUTCH-2559 protocol-http cannot handle colons after the HTTP status code
   */
  @Ignore("Fails with okhttp 3.10.0")
  @Test
  public void testHeaderWithColon() throws Exception {
    setUp();
    launchServer("HTTP/1.1 200: OK\r\n" + simpleContent);
    fetchPage("/", 200);
  }

  /**
   * NUTCH-2563 HTTP header spellchecking issues
   */
  @Test
  public void testHeaderSpellChecking() throws Exception {
    setUp();
    launchServer(responseHeader + "Client-Transfer-Encoding: chunked\r\n"
        + simpleContent);
    fetchPage("/", 200);
  }

  /**
   * NUTCH-2557 protocol-http fails to follow redirections when an HTTP response
   * body is invalid
   */
  @Ignore("Fails with okhttp 3.10.0")
  @Test
  public void testIgnoreErrorInRedirectPayload() throws Exception {
    setUp();
    launchServer("HTTP/1.1 302 Found\r\nLocation: http://example.com/\r\n"
        + "Transfer-Encoding: chunked\r\n\r\nNot a valid chunk.");
    ProtocolOutput fetched = fetchPage("/", 302);
    assertNotNull("No redirect Location.", getHeader(fetched, "Location"));
    assertEquals("Wrong redirect Location.", "http://example.com/",
        getHeader(fetched, "Location"));
  }

  /**
   * NUTCH-2558 protocol-http cannot handle a missing HTTP status line
   */
  @Ignore("Fails with okhttp 3.10.0")
  @Test
  public void testNoStatusLine() throws Exception {
    setUp();
    String text = "This is a text containing non-ASCII characters: \u00e4\u00f6\u00fc\u00df";
    launchServer(text);
    ProtocolOutput fetched = fetchPage("/", 200);
    assertEquals("Wrong text returned for response with no status line.", text,
        new String(fetched.getContent().getContent(), StandardCharsets.UTF_8));
    server.close();
    text = "<!DOCTYPE html>\n<html>\n<head>\n"
        + "<title>Testing no HTTP header èéâ</title>\n"
        + "<meta charset=\"utf-8\">\n"
        + "</head>\n<body>This is a text containing non-ASCII characters:"
        + "\u00e4\u00f6\u00fc\u00df</body>\n</html";
    launchServer(text);
    fetched = fetchPage("/", 200);
    assertEquals("Wrong text returned for response with no status line.", text,
        new String(fetched.getContent().getContent(), StandardCharsets.UTF_8));
  }

  /**
   * NUTCH-2560 protocol-http throws an error when an http header spans over
   * multiple lines
   */
  @Ignore("Fails with okhttp 3.10.0")
  @Test
  public void testMultiLineHeader() throws Exception {
    setUp();
    launchServer(responseHeader
        + "Set-Cookie: UserID=JohnDoe;\r\n  Max-Age=3600;\r\n  Version=1\r\n"
        + simpleContent);
    ProtocolOutput fetched = fetchPage("/", 200);
    LOG.info("Headers: {}", getHeaders(fetched));
    assertNotNull("Failed to set multi-line \"Set-Cookie\" header.",
        getHeader(fetched, "Set-Cookie"));
    assertTrue("Failed to set multi-line \"Set-Cookie\" header.",
        getHeader(fetched, "Set-Cookie").contains("Version=1"));
  }

  /**
   * NUTCH-2561 protocol-http can be made to read arbitrarily large HTTP
   * responses
   */
  public void testOverlongHeader() throws Exception {
    setUp();
    StringBuilder response = new StringBuilder();
    response.append(responseHeader);
    for (int i = 0; i < 80; i++) {
      response.append("X-Custom-Header-");
      for (int j = 0; j < 10000; j++) {
        response.append('x');
      }
      response.append(": hello\r\n");
    }
    response.append("\r\n" + simpleContent);
    launchServer(response.toString());
    // should throw exception because of overlong header
    fetchPage("/", -1);
  }

  /**
   * NUTCH-2562 protocol-http fails to read large chunked HTTP responses,
   * NUTCH-2575 protocol-http does not respect the maximum content-size for
   * chunked responses. Also test whether truncations of chunked content are
   * properly marked.
   */
  @Test
  public void testChunkedContent() throws Exception {
    setUp();
    StringBuilder response = new StringBuilder();
    response.append(responseHeader);
    response.append("Content-Type: text/html\r\n");
    response.append("Transfer-Encoding: chunked\r\n");
    // 81920 bytes (80 chunks, 1024 bytes each)
    // > 65536 (http.content.limit defined in nutch-site-test.xml)
    for (int i = 0; i < 80; i++) {
      response.append(String.format("\r\n400\r\n%02x\r\n", i));
      for (int j = 0; j < 1012; j++) {
        response.append('x');
      }
      response.append(String.format("\r\n%02x\r\n", i));
      response.append("\r\n");
    }
    response.append("\r\n0\r\n\r\n");
    launchServer(response.toString());
    ProtocolOutput fetched = fetchPage("/", 200);
    assertEquals(
        "Chunked content not truncated according to http.content.limit", 65536,
        fetched.getContent().getContent().length);
    assertNotNull("Content truncation not marked",
        fetched.getContent().getMetadata().get(Response.TRUNCATED_CONTENT));
    assertEquals("Content truncation not marked",
        Response.TruncatedContentReason.LENGTH.toString().toLowerCase(),
        fetched.getContent().getMetadata().get(Response.TRUNCATED_CONTENT_REASON));
  }

  /**
   * NUTCH-2729 Check for http.content.limit defined in nutch-site-test.xml:
   * whether content is truncated to the configured 64 kB and whether it is
   * properly marked as truncated.
   */
  @Test
  public void testTruncationMarking() throws Exception {
    setUp();
    int[] kBs = { 63, 64, 65 };
    for (int kB : kBs) {
      StringBuilder response = new StringBuilder();
      response.append(responseHeader);
      response.append("Content-Type: text/plain\r\nContent-Length: "
          + (kB * 1024) + "\r\n\r\n");
      for (int i = 0; i < kB; i++) {
        for (int j = 0; j < 16; j++) {
          // 16 chunks a 64 bytes = 1 kB
          response.append(
              "abcdefghijklmnopqurstuvxyz0123456789-ABCDEFGHIJKLMNOPQURSTUVXYZ\n");
        }
      }
      launchServer(response.toString());
      ProtocolOutput fetched = fetchPage("/", 200);
      assertEquals("Content not truncated according to http.content.limit",
          Math.min(kB * 1024, 65536), fetched.getContent().getContent().length);
      if (kB * 1024 > 65536) {
        assertNotNull("Content truncation not marked",
            fetched.getContent().getMetadata().get(Response.TRUNCATED_CONTENT));
        assertEquals("Content truncation not marked",
            Response.TruncatedContentReason.LENGTH.toString().toLowerCase(),
            fetched.getContent().getMetadata().get(Response.TRUNCATED_CONTENT_REASON));
      }
      server.close(); // need to close server before next loop iteration
    }
  }

  /**
   * NUTCH-2729 Check for http.content.limit defined in nutch-site-test.xml:
   * whether content is truncated to the configured 64 kB and whether it is
   * properly marked as truncated.
   */
  @Test
  public void testTruncationMarkingGzip() throws Exception {
    setUp();
    int[] kBs = { 63, 64, 65 };
    for (int kB : kBs) {
      StringBuilder payload = new StringBuilder();
      for (int i = 0; i < kB; i++) {
        for (int j = 0; j < 16; j++) {
          // 16 chunks a 64 bytes = 1 kB
          payload.append(
              "abcdefghijklmnopqurstuvxyz0123456789-ABCDEFGHIJKLMNOPQURSTUVXYZ\n");
        }
      }
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      GZIPOutputStream gzip = new GZIPOutputStream(bytes);
      gzip.write(payload.toString().getBytes(StandardCharsets.UTF_8));
      gzip.close();
      StringBuilder responseHead = new StringBuilder();
      responseHead.append(responseHeader);
      responseHead.append("Content-Type: text/plain\r\nContent-Length: "
          + bytes.size() + "\r\nContent-Encoding: gzip\r\n\r\n");
      ByteArrayOutputStream response = new ByteArrayOutputStream();
      response.write(responseHead.toString().getBytes(StandardCharsets.UTF_8));
      response.write(bytes.toByteArray());

      launchServer(response.toByteArray());
      ProtocolOutput fetched = fetchPage("/", 200);
      assertEquals("Content not truncated according to http.content.limit",
          Math.min(kB * 1024, 65536), fetched.getContent().getContent().length);
      if (kB * 1024 > 65536) {
        assertNotNull("Content truncation not marked",
            fetched.getContent().getMetadata().get(Response.TRUNCATED_CONTENT));
        assertEquals("Content truncation not marked",
            Response.TruncatedContentReason.LENGTH.toString().toLowerCase(),
            fetched.getContent().getMetadata().get(Response.TRUNCATED_CONTENT_REASON));
      }
      server.close(); // need to close server before next loop iteration
    }
  }

  /**
   * Force an exception after all content has been fetched by sending a wrong
   * `Content-Length` header and check whether the content is stored anyway if
   * http.partial.truncated == true
   */
  @Test
  public void testPartialContentTruncated() throws Exception {
    setUp();
    conf.setBoolean("http.partial.truncated", true);
    http.setConf(conf);
    String testContent = "This is a text.";
    launchServer(
        responseHeader + "Content-Length: 50000\r\n\r\n" + testContent);
    ProtocolOutput fetched = fetchPage("/", 200);
    assertEquals("Content not saved as truncated", testContent,
        new String(fetched.getContent().getContent(), StandardCharsets.UTF_8));
    assertNotNull("Content truncation not marked",
        fetched.getContent().getMetadata().get(Response.TRUNCATED_CONTENT));
  }

  @Test
  public void testNoContentLimit() throws Exception {
    setUp();
    conf.setInt("http.content.limit", -1);
    http.setConf(conf);
    StringBuilder response = new StringBuilder();
    response.append(responseHeader);
    // Even 128 kB content shouldn't cause any truncation because
    // http.content.limit == -1
    int kB = 128;
    response.append("Content-Type: text/plain\r\nContent-Length: " + (kB * 1024)
        + "\r\n\r\n");
    for (int i = 0; i < kB; i++) {
      for (int j = 0; j < 16; j++) {
        // 16 chunks a 64 bytes = 1 kB
        response.append(
            "abcdefghijklmnopqurstuvxyz0123456789-ABCDEFGHIJKLMNOPQURSTUVXYZ\n");
      }
    }
    launchServer(response.toString());
    ProtocolOutput fetched = fetchPage("/", 200);
    assertEquals("Content truncated although http.content.limit == -1",
        (kB * 1024), fetched.getContent().getContent().length);
  }

}
