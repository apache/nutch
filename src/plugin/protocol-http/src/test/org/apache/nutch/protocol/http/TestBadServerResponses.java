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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;

import org.apache.nutch.protocol.AbstractHttpProtocolPluginTest;
import org.apache.nutch.protocol.ProtocolOutput;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test cases for protocol-http - robustness regarding bad server responses:
 * malformed HTTP header lines, etc. See, NUTCH-2549.
 */
public class TestBadServerResponses extends AbstractHttpProtocolPluginTest {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  @Override
  protected String getPluginClassName() {
    return "org.apache.nutch.protocol.http.Http";
  }

  @Test
  public void testBadHttpServer() throws Exception {
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
    launchServer("/?171", responseHeader + simpleContent);
    // request ?171 should be normalized to /?171
    fetchPage("?171", 200);
  }

  /**
   * NUTCH-2564 protocol-http throws an error when the content-length header is
   * not a number
   */
  @Test
  public void testContentLengthNotANumber() throws Exception {
    launchServer(
        responseHeader + "Content-Length: thousand\r\n" + simpleContent);
    fetchPage("/", 200);
  }

  /**
   * NUTCH-2559 protocol-http cannot handle colons after the HTTP status code
   */
  @Test
  public void testHeaderWithColon() throws Exception {
    launchServer("HTTP/1.1 200: OK\r\n" + simpleContent);
    fetchPage("/", 200);
  }

  /**
   * NUTCH-2563 HTTP header spellchecking issues
   */
  @Test
  public void testHeaderSpellChecking() throws Exception {
    launchServer(responseHeader + "Client-Transfer-Encoding: chunked\r\n"
        + simpleContent);
    fetchPage("/", 200);
  }

  /**
   * NUTCH-2557 protocol-http fails to follow redirections when an HTTP response
   * body is invalid
   */
  @Test
  public void testIgnoreErrorInRedirectPayload() throws Exception {
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
  @Test
  public void testNoStatusLine() throws Exception {
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
  @Test
  public void testMultiLineHeader() throws Exception {
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
   * chunked responses
   */
  @Test
  public void testChunkedContent() throws Exception {
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
  }

}
