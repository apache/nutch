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
package org.apache.nutch.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.storage.WebPage;

public class TestEncodingDetector {
  private static Configuration conf = NutchConfiguration.create();

  private static byte[] contentInOctets;

  static {
    try {
      contentInOctets = "çñôöøДЛжҶ".getBytes("utf-8");
    } catch (UnsupportedEncodingException e) {
      // not possible
    }
  }

  @Test
  public void testGuessing() {
    // first disable auto detection
    conf.setInt(EncodingDetector.MIN_CONFIDENCE_KEY, -1);

    //Metadata metadata = new Metadata();
    EncodingDetector detector;
    // Content content;
    String encoding;

    WebPage page = new WebPage();
    page.setBaseUrl(new Utf8("http://www.example.com/"));
    page.setContentType(new Utf8("text/plain"));
    page.setContent(ByteBuffer.wrap(contentInOctets));

    detector = new EncodingDetector(conf);
    detector.autoDetectClues(page, true);
    encoding = detector.guessEncoding(page, "windows-1252");
    // no information is available, so it should return default encoding
    assertEquals("windows-1252", encoding.toLowerCase());

    page = new WebPage();
    page.setBaseUrl(new Utf8("http://www.example.com/"));
    page.setContentType(new Utf8("text/plain"));
    page.setContent(ByteBuffer.wrap(contentInOctets));
    page.putToHeaders(EncodingDetector.CONTENT_TYPE_UTF8, new Utf8("text/plain; charset=UTF-16"));
    
    detector = new EncodingDetector(conf);
    detector.autoDetectClues(page, true);
    encoding = detector.guessEncoding(page, "windows-1252");
    assertEquals("utf-16", encoding.toLowerCase());

    page = new WebPage();
    page.setBaseUrl(new Utf8("http://www.example.com/"));
    page.setContentType(new Utf8("text/plain"));
    page.setContent(ByteBuffer.wrap(contentInOctets));
    
    detector = new EncodingDetector(conf);
    detector.autoDetectClues(page, true);
    detector.addClue("windows-1254", "sniffed");
    encoding = detector.guessEncoding(page, "windows-1252");
    assertEquals("windows-1254", encoding.toLowerCase());

    // enable autodetection
    conf.setInt(EncodingDetector.MIN_CONFIDENCE_KEY, 50);
    page = new WebPage();
    page.setBaseUrl(new Utf8("http://www.example.com/"));
    page.setContentType(new Utf8("text/plain"));
    page.setContent(ByteBuffer.wrap(contentInOctets));
    page.putToMetadata(new Utf8(Response.CONTENT_TYPE), ByteBuffer.wrap("text/plain; charset=UTF-16".getBytes()));
    
    detector = new EncodingDetector(conf);
    detector.autoDetectClues(page, true);
    detector.addClue("utf-32", "sniffed");
    encoding = detector.guessEncoding(page, "windows-1252");
    assertEquals("utf-8", encoding.toLowerCase());
  }

}
