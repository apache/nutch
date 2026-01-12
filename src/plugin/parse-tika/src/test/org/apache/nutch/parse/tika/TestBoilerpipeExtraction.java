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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Boilerpipe content extraction integration with Tika.
 * These tests help ensure boilerpipe support works correctly across Tika upgrades.
 */
public class TestBoilerpipeExtraction {

  private static final String HTML_WITH_BOILERPLATE = 
      "<!DOCTYPE html><html><head><title>Article Title</title></head><body>"
      + "<div id='header'>Navigation | Menu | Links</div>"
      + "<div id='content'>"
      + "<h1>Main Article Heading</h1>"
      + "<p>This is the main article content that should be extracted. "
      + "It contains important information about the topic.</p>"
      + "<p>Another paragraph with meaningful content that adds value.</p>"
      + "</div>"
      + "<div id='footer'>Copyright 2024 | Terms | Privacy</div>"
      + "</body></html>";

  @Test
  public void testBoilerpipeExtraction() {
    Configuration conf = NutchConfiguration.create();
    conf.set("plugin.includes", "parse-tika");
    conf.set("tika.extractor", "boilerpipe");
    conf.set("tika.extractor.boilerpipe.algorithm", "ArticleExtractor");
    
    TikaParser parser = new TikaParser();
    parser.setConf(conf);
    
    String url = "http://example.com/article.html";
    Content content = new Content(url, url, 
        HTML_WITH_BOILERPLATE.getBytes(), "text/html", new Metadata(), conf);
    
    Parse parse = parser.getParse(content).get(url);
    String text = parse.getText();
    
    // Boilerpipe should extract main content
    assertTrue(text.contains("Main Article Heading"), 
        "Should contain article heading");
    assertTrue(text.contains("main article content"), 
        "Should contain main content");
  }
  
  @Test
  public void testBoilerpipeDisabled() {
    Configuration conf = NutchConfiguration.create();
    conf.set("plugin.includes", "parse-tika");
    conf.set("tika.extractor", "none"); // Default - boilerpipe disabled
    
    TikaParser parser = new TikaParser();
    parser.setConf(conf);
    
    String url = "http://example.com/article.html";
    Content content = new Content(url, url, 
        HTML_WITH_BOILERPLATE.getBytes(), "text/html", new Metadata(), conf);
    
    Parse parse = parser.getParse(content).get(url);
    String text = parse.getText();
    
    // Without boilerpipe, all text should be extracted including boilerplate
    assertTrue(text.contains("Navigation"), "Should contain navigation text");
    assertTrue(text.contains("Copyright"), "Should contain footer text");
    assertTrue(text.contains("Main Article Heading"), "Should contain main content");
  }

  @Test
  public void testBoilerpipeWithNonHtmlMimeType() {
    Configuration conf = NutchConfiguration.create();
    conf.set("plugin.includes", "parse-tika");
    conf.set("tika.extractor", "boilerpipe");
    conf.set("tika.extractor.boilerpipe.algorithm", "ArticleExtractor");
    // Only apply to HTML mime types
    conf.setStrings("tika.extractor.boilerpipe.mime.types", "text/html");
    
    TikaParser parser = new TikaParser();
    parser.setConf(conf);
    
    // Using XHTML mime type which is not in the configured list
    String url = "http://example.com/article.xhtml";
    Content content = new Content(url, url, 
        HTML_WITH_BOILERPLATE.getBytes(), "application/xhtml+xml", new Metadata(), conf);
    
    Parse parse = parser.getParse(content).get(url);
    assertNotNull(parse, "Should parse successfully even when boilerpipe doesn't apply");
  }
}
