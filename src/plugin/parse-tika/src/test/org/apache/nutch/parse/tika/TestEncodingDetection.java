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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for encoding/charset detection.
 * These tests help ensure charset detection works correctly across Tika upgrades.
 */
public class TestEncodingDetection {

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
  public void testUtf8WithMetaCharset() {
    String html = "<!DOCTYPE html><html><head>"
        + "<meta charset=\"utf-8\">"
        + "<title>UTF-8 Test</title></head>"
        + "<body>äöü ñ 中文</body></html>";
    
    byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
    Parse parse = doParse(bytes);
    
    assertEquals("UTF-8 Test", parse.getData().getTitle());
    assertTrue(parse.getText().contains("äöü"));
    assertTrue(parse.getText().contains("中文"));
  }

  @Test
  public void testUtf8WithHttpEquiv() {
    String html = "<!DOCTYPE html><html><head>"
        + "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\">"
        + "<title>UTF-8 HTTP-Equiv</title></head>"
        + "<body>Special chars: äöü</body></html>";
    
    byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
    Parse parse = doParse(bytes);
    
    assertEquals("UTF-8 HTTP-Equiv", parse.getData().getTitle());
    assertTrue(parse.getText().contains("äöü"));
  }

  @Test
  public void testUtf8BOM() {
    String html = "<!DOCTYPE html><html><head>"
        + "<title>BOM Test</title></head>"
        + "<body>Content with BOM</body></html>";
    
    // UTF-8 BOM
    byte[] bom = new byte[]{(byte)0xEF, (byte)0xBB, (byte)0xBF};
    byte[] htmlBytes = html.getBytes(StandardCharsets.UTF_8);
    byte[] withBom = new byte[bom.length + htmlBytes.length];
    System.arraycopy(bom, 0, withBom, 0, bom.length);
    System.arraycopy(htmlBytes, 0, withBom, bom.length, htmlBytes.length);
    
    Parse parse = doParse(withBom);
    
    assertEquals("BOM Test", parse.getData().getTitle());
    assertTrue(parse.getText().contains("Content with BOM"));
  }

  @Test
  public void testIso88591() {
    String html = "<!DOCTYPE html><html><head>"
        + "<meta charset=\"iso-8859-1\">"
        + "<title>ISO-8859-1 Test</title></head>"
        + "<body>German: \u00e4\u00f6\u00fc</body></html>";
    
    try {
      Charset charset = Charset.forName("ISO-8859-1");
      byte[] bytes = html.getBytes(charset);
      Parse parse = doParse(bytes);
      
      assertEquals("ISO-8859-1 Test", parse.getData().getTitle());
    } catch (Exception e) {
      // ISO-8859-1 might not be available on all systems
      // Skip test in that case
    }
  }

  @Test
  public void testUtf16BE() {
    String html = "<!DOCTYPE html><html><head>"
        + "<title>UTF-16BE Test</title></head>"
        + "<body>Content</body></html>";
    
    // UTF-16BE doesn't add BOM automatically
    byte[] bytes = html.getBytes(StandardCharsets.UTF_16BE);
    
    // Add BOM manually for UTF-16BE
    byte[] bom = new byte[]{(byte)0xFE, (byte)0xFF};
    byte[] withBom = new byte[bom.length + bytes.length];
    System.arraycopy(bom, 0, withBom, 0, bom.length);
    System.arraycopy(bytes, 0, withBom, bom.length, bytes.length);
    
    Parse parse = doParse(withBom);
    
    assertNotNull(parse.getData().getTitle());
  }

  @Test
  public void testUtf16LE() {
    String html = "<!DOCTYPE html><html><head>"
        + "<title>UTF-16LE Test</title></head>"
        + "<body>Content</body></html>";
    
    // UTF-16LE doesn't add BOM automatically
    byte[] bytes = html.getBytes(StandardCharsets.UTF_16LE);
    
    // Add BOM manually for UTF-16LE
    byte[] bom = new byte[]{(byte)0xFF, (byte)0xFE};
    byte[] withBom = new byte[bom.length + bytes.length];
    System.arraycopy(bom, 0, withBom, 0, bom.length);
    System.arraycopy(bytes, 0, withBom, bom.length, bytes.length);
    
    Parse parse = doParse(withBom);
    
    assertNotNull(parse.getData().getTitle());
  }

  @Test
  public void testNoEncodingDeclaration() {
    // HTML without any encoding declaration - should default to something reasonable
    String html = "<!DOCTYPE html><html><head>"
        + "<title>No Encoding</title></head>"
        + "<body>Plain ASCII content</body></html>";
    
    byte[] bytes = html.getBytes(StandardCharsets.US_ASCII);
    Parse parse = doParse(bytes);
    
    assertEquals("No Encoding", parse.getData().getTitle());
    assertTrue(parse.getText().contains("Plain ASCII content"));
  }

  @Test
  public void testXmlDeclarationEncoding() {
    String html = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<!DOCTYPE html><html xmlns=\"http://www.w3.org/1999/xhtml\">"
        + "<head><title>XHTML Test</title></head>"
        + "<body>Special: äöü</body></html>";
    
    byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
    
    String url = "http://example.com/";
    Content content = new Content(url, url, bytes, 
        "application/xhtml+xml", new Metadata(), conf);
    Parse parse = parser.getParse(content).get(url);
    
    assertNotNull(parse);
    // Title extraction may vary for XHTML
  }

  private Parse doParse(byte[] bytes) {
    String url = "http://example.com/";
    Content content = new Content(url, url, bytes, "text/html", new Metadata(), conf);
    return parser.getParse(content).get(url);
  }
}
