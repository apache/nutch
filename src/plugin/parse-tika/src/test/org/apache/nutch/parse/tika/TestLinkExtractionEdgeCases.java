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
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for link extraction edge cases.
 * These tests help catch behavior changes in Tika's link extraction across versions.
 */
public class TestLinkExtractionEdgeCases {

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
  public void testDuplicateLinkDeduplication() {
    // Same URL appearing multiple times should be deduplicated
    String html = "<html><body>"
        + "<a href='http://example.com/page'>Link 1</a>"
        + "<a href='http://example.com/page'>Link 2</a>"
        + "<a href='http://example.com/page'>Link 3</a>"
        + "</body></html>";
    
    Outlink[] outlinks = parseOutlinks(html);
    
    // Should deduplicate to single link
    assertEquals(1, outlinks.length, "Duplicate URLs should be deduplicated");
    assertEquals("http://example.com/page", outlinks[0].getToUrl());
  }

  @Test
  public void testLinkOrderPreserved() {
    String html = "<html><body>"
        + "<a href='http://example.com/first'>First</a>"
        + "<a href='http://example.com/second'>Second</a>"
        + "<a href='http://example.com/third'>Third</a>"
        + "</body></html>";
    
    Outlink[] outlinks = parseOutlinks(html);
    
    assertEquals(3, outlinks.length, "Should extract all 3 links");
    assertEquals("http://example.com/first", outlinks[0].getToUrl(), 
        "First link should be first");
    assertEquals("http://example.com/second", outlinks[1].getToUrl(), 
        "Second link should be second");
    assertEquals("http://example.com/third", outlinks[2].getToUrl(), 
        "Third link should be third");
  }

  @Test
  public void testNestedAnchors() {
    // Malformed HTML with nested anchors
    String html = "<html><body>"
        + "<a href='/outer'><a href='/inner'>Nested</a></a>"
        + "</body></html>";
    
    Outlink[] outlinks = parseOutlinks(html);
    
    // At minimum, inner link should be extracted
    Set<String> urls = Arrays.stream(outlinks)
        .map(Outlink::getToUrl)
        .collect(Collectors.toSet());
    assertTrue(urls.contains("http://example.com/inner"), 
        "Inner link should be extracted");
  }

  @Test
  public void testRelativeLinks() {
    String html = "<html><body>"
        + "<a href='page.html'>Relative</a>"
        + "<a href='./page.html'>Dot Relative</a>"
        + "<a href='../parent.html'>Parent</a>"
        + "<a href='/absolute.html'>Absolute Path</a>"
        + "</body></html>";
    
    Outlink[] outlinks = parseOutlinks(html, "http://example.com/dir/index.html");
    
    Set<String> urls = Arrays.stream(outlinks)
        .map(Outlink::getToUrl)
        .collect(Collectors.toSet());
    
    assertTrue(urls.contains("http://example.com/dir/page.html") || 
               urls.contains("http://example.com/page.html"),
        "Should resolve relative link");
    assertTrue(urls.contains("http://example.com/absolute.html"),
        "Should resolve absolute path");
  }

  @Test
  public void testAnchorTextExtraction() {
    String html = "<html><body>"
        + "<a href='http://example.com/page'>  Anchor   Text  </a>"
        + "</body></html>";
    
    Outlink[] outlinks = parseOutlinks(html);
    
    assertEquals(1, outlinks.length);
    // Anchor text should be trimmed and whitespace normalized
    assertEquals("Anchor Text", outlinks[0].getAnchor().trim().replaceAll("\\s+", " "));
  }

  @Test
  public void testEmptyAnchorText() {
    String html = "<html><body>"
        + "<a href='http://example.com/page'></a>"
        + "<a href='http://example.com/page2'>   </a>"
        + "</body></html>";
    
    Outlink[] outlinks = parseOutlinks(html);
    
    // Links should still be extracted even with empty anchor text
    assertTrue(outlinks.length >= 1, "Should extract links with empty anchors");
  }

  @Test
  public void testImageLinks() {
    String html = "<html><body>"
        + "<a href='http://example.com/page'><img src='image.jpg' alt='Image Alt'></a>"
        + "</body></html>";
    
    Outlink[] outlinks = parseOutlinks(html);
    
    // Should extract the anchor link (Tika may also extract image src as separate link)
    Set<String> urls = Arrays.stream(outlinks)
        .map(Outlink::getToUrl)
        .collect(Collectors.toSet());
    assertTrue(urls.contains("http://example.com/page"), 
        "Should extract the anchor link");
  }

  @Test
  public void testFormActionNotExtracted() {
    conf.setBoolean("parser.html.form.use_action", false);
    parser.setConf(conf);
    
    String html = "<html><body>"
        + "<form action='http://example.com/submit' method='post'>"
        + "<input type='submit'/>"
        + "</form>"
        + "<a href='http://example.com/link'>Real Link</a>"
        + "</body></html>";
    
    Outlink[] outlinks = parseOutlinks(html);
    
    // Form action should not be extracted as outlink
    Set<String> urls = Arrays.stream(outlinks)
        .map(Outlink::getToUrl)
        .collect(Collectors.toSet());
    assertFalse(urls.contains("http://example.com/submit"),
        "Form action should not be extracted when disabled");
    assertTrue(urls.contains("http://example.com/link"),
        "Regular links should still be extracted");
  }

  @Test
  public void testFragmentLinks() {
    String html = "<html><body>"
        + "<a href='#section'>Fragment Only</a>"
        + "<a href='page.html#section'>Page with Fragment</a>"
        + "</body></html>";
    
    Outlink[] outlinks = parseOutlinks(html);
    
    // Fragment-only links typically not extracted, page links should be
    Set<String> urls = Arrays.stream(outlinks)
        .map(Outlink::getToUrl)
        .collect(Collectors.toSet());
    
    // The page link should be extracted (with or without fragment)
    boolean hasPageLink = urls.stream()
        .anyMatch(u -> u.contains("page.html"));
    assertTrue(hasPageLink, "Page links should be extracted");
  }

  private Outlink[] parseOutlinks(String html) {
    return parseOutlinks(html, "http://example.com/");
  }

  private Outlink[] parseOutlinks(String html, String baseUrl) {
    Content content = new Content(baseUrl, baseUrl, html.getBytes(), 
        "text/html", new Metadata(), conf);
    Parse parse = parser.getParse(content).get(baseUrl);
    return parse.getData().getOutlinks();
  }
}
