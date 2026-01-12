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
package org.apache.nutch.parse.tika;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for parser failure handling and graceful degradation.
 * These tests ensure the parser handles edge cases without throwing exceptions.
 */
public class TestParserFailureHandling {

  private Configuration conf;
  private TikaParser parser;

  @BeforeEach
  public void setup() {
    conf = NutchConfiguration.create();
    conf.set("plugin.includes", "parse-tika");
    parser = new TikaParser();
    parser.setConf(conf);
  }

  @Test
  public void testMalformedHtml() {
    // Severely malformed HTML
    String html = "<<<>>>not really html at all{{{";
    
    ParseResult result = doParse(html, "text/html");
    
    // Should return a result even for malformed input (not throw exception)
    assertNotNull(result, "Should return a result even for malformed input");
  }

  @Test
  public void testEmptyContent() {
    ParseResult result = doParse("", "text/html");
    
    // Should handle empty content gracefully
    assertNotNull(result, "Should handle empty content");
  }

  @Test
  public void testNullBytes() {
    String url = "http://example.com/";
    Content content = new Content(url, url, new byte[0], 
        "text/html", new Metadata(), conf);
    
    ParseResult result = parser.getParse(content);
    
    // Should handle zero-length byte array
    assertNotNull(result, "Should handle zero-length content");
  }

  @Test
  public void testBinaryContentAsHtml() {
    // Random binary content declared as HTML
    byte[] binaryContent = new byte[]{
        0x00, 0x01, 0x02, (byte)0xFF, (byte)0xFE, 0x50, 0x4B, 0x03, 0x04
    };
    
    String url = "http://example.com/";
    Content content = new Content(url, url, binaryContent, 
        "text/html", new Metadata(), conf);
    
    // Should not throw exception
    ParseResult result = parser.getParse(content);
    assertNotNull(result, "Should handle binary content declared as HTML");
  }

  @Test
  public void testUnknownMimeType() {
    String url = "http://example.com/file.xyz";
    Content content = new Content(url, url, "some content".getBytes(), 
        "application/x-unknown-type", new Metadata(), conf);
    
    ParseResult result = parser.getParse(content);
    
    // Should handle gracefully (may return failed status but not throw)
    assertNotNull(result, "Should handle unknown MIME type");
  }

  @Test
  public void testVeryLongTitle() {
    StringBuilder sb = new StringBuilder();
    sb.append("<!DOCTYPE html><html><head><title>");
    for (int i = 0; i < 10000; i++) {
      sb.append("VeryLongTitle");
    }
    sb.append("</title></head><body>Content</body></html>");
    
    ParseResult result = doParse(sb.toString(), "text/html");
    
    assertNotNull(result, "Should handle very long titles");
  }

  @Test
  public void testDeeplyNestedHtml() {
    StringBuilder sb = new StringBuilder();
    sb.append("<!DOCTYPE html><html><body>");
    // Create deeply nested divs
    for (int i = 0; i < 500; i++) {
      sb.append("<div>");
    }
    sb.append("Deep content");
    for (int i = 0; i < 500; i++) {
      sb.append("</div>");
    }
    sb.append("</body></html>");
    
    ParseResult result = doParse(sb.toString(), "text/html");
    
    assertNotNull(result, "Should handle deeply nested HTML");
  }

  @Test
  public void testManyLinks() {
    StringBuilder sb = new StringBuilder();
    sb.append("<!DOCTYPE html><html><body>");
    // Create many links
    for (int i = 0; i < 1000; i++) {
      sb.append("<a href='http://example.com/page").append(i).append("'>Link ").append(i).append("</a>");
    }
    sb.append("</body></html>");
    
    ParseResult result = doParse(sb.toString(), "text/html");
    
    assertNotNull(result, "Should handle many links");
  }

  @Test
  public void testSpecialCharactersInUrls() {
    String html = "<!DOCTYPE html><html><body>"
        + "<a href='http://example.com/path?q=hello world&foo=bar'>Link 1</a>"
        + "<a href='http://example.com/path?q=hello%20world'>Link 2</a>"
        + "<a href='http://example.com/path#section'>Link 3</a>"
        + "<a href='http://example.com/路径'>Link 4</a>"
        + "</body></html>";
    
    ParseResult result = doParse(html, "text/html");
    
    assertNotNull(result, "Should handle special characters in URLs");
  }

  @Test
  public void testNullCharactersInContent() {
    // HTML with null characters embedded
    byte[] htmlBytes = "<!DOCTYPE html><html><body>Before\0After</body></html>".getBytes();
    
    String url = "http://example.com/";
    Content content = new Content(url, url, htmlBytes, 
        "text/html", new Metadata(), conf);
    
    ParseResult result = parser.getParse(content);
    
    assertNotNull(result, "Should handle null characters in content");
  }

  @Test
  public void testIncompleteHtml() {
    // HTML that cuts off mid-tag
    String html = "<!DOCTYPE html><html><head><title>Test</title></head><body><div>Content<a href='http://";
    
    ParseResult result = doParse(html, "text/html");
    
    assertNotNull(result, "Should handle incomplete HTML");
  }

  @Test
  public void testHtmlWithScriptErrors() {
    String html = "<!DOCTYPE html><html><head>"
        + "<script>this is { not valid javascript</script>"
        + "<title>Script Error</title>"
        + "</head><body>Content</body></html>";
    
    ParseResult result = doParse(html, "text/html");
    
    assertNotNull(result, "Should handle invalid JavaScript in script tags");
  }

  @Test
  public void testHtmlWithCssErrors() {
    String html = "<!DOCTYPE html><html><head>"
        + "<style>this is { not: valid; css }</style>"
        + "<title>CSS Error</title>"
        + "</head><body>Content</body></html>";
    
    ParseResult result = doParse(html, "text/html");
    
    assertNotNull(result, "Should handle invalid CSS in style tags");
  }

  private ParseResult doParse(String content, String mimeType) {
    String url = "http://example.com/";
    Content c = new Content(url, url, content.getBytes(), 
        mimeType, new Metadata(), conf);
    return parser.getParse(c);
  }
}
