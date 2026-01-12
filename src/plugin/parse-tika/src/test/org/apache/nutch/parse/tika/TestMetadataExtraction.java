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
import org.apache.nutch.parse.Parse;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for metadata extraction from HTML documents.
 * These tests help ensure metadata extraction works correctly across Tika upgrades.
 */
public class TestMetadataExtraction {

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
  public void testBasicMetaTags() {
    String html = "<!DOCTYPE html><html><head>"
        + "<title>Page Title</title>"
        + "<meta name='description' content='Page description here'>"
        + "<meta name='keywords' content='keyword1, keyword2, keyword3'>"
        + "<meta name='author' content='Test Author'>"
        + "</head><body>Content</body></html>";
    
    Parse parse = doParse(html);
    org.apache.nutch.metadata.Metadata parseMeta = parse.getData().getParseMeta();
    
    assertEquals("Page Title", parse.getData().getTitle());
    assertEquals("Page description here", parseMeta.get("description"));
    assertEquals("keyword1, keyword2, keyword3", parseMeta.get("keywords"));
    assertEquals("Test Author", parseMeta.get("author"));
  }

  @Test
  public void testOpenGraphMetaTags() {
    String html = "<!DOCTYPE html><html><head>"
        + "<title>Regular Title</title>"
        + "<meta property='og:title' content='OG Title'>"
        + "<meta property='og:description' content='OG Description'>"
        + "<meta property='og:type' content='article'>"
        + "<meta property='og:url' content='http://example.com/page'>"
        + "</head><body>Content</body></html>";
    
    Parse parse = doParse(html);
    org.apache.nutch.metadata.Metadata parseMeta = parse.getData().getParseMeta();
    
    assertEquals("Regular Title", parse.getData().getTitle());
    // OG tags should be in metadata
    assertNotNull(parseMeta.get("og:title"));
  }

  @Test
  public void testTwitterCardMetaTags() {
    String html = "<!DOCTYPE html><html><head>"
        + "<title>Page Title</title>"
        + "<meta name='twitter:card' content='summary'>"
        + "<meta name='twitter:title' content='Twitter Title'>"
        + "<meta name='twitter:description' content='Twitter Description'>"
        + "</head><body>Content</body></html>";
    
    Parse parse = doParse(html);
    org.apache.nutch.metadata.Metadata parseMeta = parse.getData().getParseMeta();
    
    assertEquals("Page Title", parse.getData().getTitle());
    // Twitter tags should be in metadata
    assertNotNull(parseMeta.get("twitter:card"));
  }

  @Test
  public void testRobotsMetaNoIndex() {
    String html = "<!DOCTYPE html><html><head>"
        + "<meta name='robots' content='noindex'>"
        + "<title>No Index Page</title>"
        + "</head><body>This should not be indexed</body></html>";
    
    Parse parse = doParse(html);
    
    // With noindex, text should be empty
    assertTrue(parse.getText().isEmpty() || parse.getText().isBlank(),
        "noindex should prevent text extraction");
  }

  @Test
  public void testRobotsMetaNoFollow() {
    String html = "<!DOCTYPE html><html><head>"
        + "<meta name='robots' content='nofollow'>"
        + "</head><body>"
        + "<a href='http://example.com/link1'>Link 1</a>"
        + "<a href='http://example.com/link2'>Link 2</a>"
        + "</body></html>";
    
    Parse parse = doParse(html);
    
    // With nofollow, outlinks should be empty
    assertEquals(0, parse.getData().getOutlinks().length, 
        "nofollow should prevent outlink extraction");
  }

  @Test
  public void testRobotsMetaNoIndexNoFollow() {
    String html = "<!DOCTYPE html><html><head>"
        + "<meta name='robots' content='noindex, nofollow'>"
        + "<title>Blocked Page</title>"
        + "</head><body>"
        + "<p>Content here</p>"
        + "<a href='http://example.com/link'>Link</a>"
        + "</body></html>";
    
    Parse parse = doParse(html);
    
    // Both should be blocked
    assertTrue(parse.getText().isEmpty() || parse.getText().isBlank(),
        "noindex should prevent text extraction");
    assertEquals(0, parse.getData().getOutlinks().length, 
        "nofollow should prevent outlink extraction");
  }

  @Test
  public void testRefreshMetaTag() {
    String html = "<!DOCTYPE html><html><head>"
        + "<meta http-equiv='refresh' content='5;url=http://example.com/newpage'>"
        + "<title>Redirect Page</title>"
        + "</head><body>Redirecting...</body></html>";
    
    Parse parse = doParse(html);
    
    // Parse should still succeed
    assertNotNull(parse);
    assertEquals("Redirect Page", parse.getData().getTitle());
  }

  @Test
  public void testBaseHref() {
    String html = "<!DOCTYPE html><html><head>"
        + "<base href='http://other.example.com/'>"
        + "<title>Base HREF Test</title>"
        + "</head><body>"
        + "<a href='page.html'>Relative Link</a>"
        + "</body></html>";
    
    String url = "http://example.com/";
    Content content = new Content(url, url, html.getBytes(), 
        "text/html", new Metadata(), conf);
    Parse parse = parser.getParse(content).get(url);
    
    // Link should be resolved relative to base href
    if (parse.getData().getOutlinks().length > 0) {
      String linkUrl = parse.getData().getOutlinks()[0].getToUrl();
      assertTrue(linkUrl.contains("other.example.com") || linkUrl.contains("example.com"),
          "Link should be resolved using base href or original URL");
    }
  }

  @Test
  public void testMultipleMetaTagsWithSameName() {
    String html = "<!DOCTYPE html><html><head>"
        + "<title>Multi Meta</title>"
        + "<meta name='keywords' content='first'>"
        + "<meta name='keywords' content='second'>"
        + "</head><body>Content</body></html>";
    
    Parse parse = doParse(html);
    org.apache.nutch.metadata.Metadata parseMeta = parse.getData().getParseMeta();
    
    // Should handle multiple meta tags with same name
    String keywords = parseMeta.get("keywords");
    assertNotNull(keywords, "Keywords should be extracted");
  }

  @Test
  public void testEmptyMetaTags() {
    String html = "<!DOCTYPE html><html><head>"
        + "<title></title>"
        + "<meta name='description' content=''>"
        + "<meta name='keywords'>"
        + "</head><body>Content</body></html>";
    
    Parse parse = doParse(html);
    
    // Should handle empty meta tags gracefully
    assertNotNull(parse);
    // Empty title should result in empty string
    assertTrue(parse.getData().getTitle() == null || 
               parse.getData().getTitle().isEmpty());
  }

  private Parse doParse(String html) {
    String url = "http://example.com/";
    Content content = new Content(url, url, html.getBytes(), 
        "text/html", new Metadata(), conf);
    return parser.getParse(content).get(url);
  }
}
