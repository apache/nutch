/**
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
package org.apache.nutch.indexer.basic;

import java.nio.ByteBuffer;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * JUnit test case which tests
 * 1. that the host, url, orig, content, title, cache and tstamp fields 
 * are obtained by the filter.
 * 2. that configurable maximum length functionality for titles actually works. .
 * This property defaults at 100 characters @see {@code indexer.max.title.length} 
 * in nutch-default.xml but has been set to 10 for this test.
 * 
 * @author lewismc
 */

public class TestBasicIndexingFilter {
  
  @Test
  public void testBasicFields() throws Exception {
	Configuration conf = NutchConfiguration.create();
	BasicIndexingFilter filter = new BasicIndexingFilter();
	filter.setConf(conf);
	assertNotNull(filter);
	NutchDocument doc = new NutchDocument();
	WebPage page = new WebPage();
	page.putToInlinks(new Utf8("http://nutch.apache.org/"), new Utf8("Welcome to Nutch"));
	page.setTitle(new Utf8("Welcome to Nutch"));
    page.setReprUrl(new Utf8("http://www.urldoesnotmatter.org"));
    byte[] bytes = new byte[10];
    ByteBuffer bbuf = ByteBuffer.wrap(bytes);
    page.putToMetadata(Nutch.CACHING_FORBIDDEN_KEY_UTF8, bbuf);
    page.setFetchTime(System.currentTimeMillis());
	try {
	  filter.filter(doc, "http://www.apache.org/", page);
	} catch(Exception e) {
	  e.printStackTrace();
	  fail(e.getMessage());
	}
	assertNotNull(doc);
	assertTrue("check for host field ", doc.getFieldNames().contains("host"));
	assertTrue("check for url field", doc.getFieldNames().contains("url"));
	assertTrue("check for orig field", doc.getFieldNames().contains("orig"));
	assertTrue("check for content field", doc.getFieldNames().contains("content"));
	assertTrue("check for title field", doc.getFieldNames().contains("title"));
	assertTrue("check for cache field", doc.getFieldNames().contains("cache"));
	assertTrue("check for tstamp field", doc.getFieldNames().contains("tstamp"));
  }
  
  @Test
  public void testTitleFieldLength() throws Exception {
	Configuration conf = NutchConfiguration.create();
	conf.setInt("indexer.max.title.length", 10);
	BasicIndexingFilter filter = new BasicIndexingFilter();
	filter.setConf(conf);
	assertNotNull(filter);
	NutchDocument doc = new NutchDocument();
	WebPage page = new WebPage();
	page.putToInlinks(new Utf8("http://exceedmaximumtitleurl.org/"), new Utf8("exceeding title site"));
	page.setTitle(new Utf8("This title exceeds maximum characters"));
	try {
	  filter.filter(doc, "http://www.apache.org/", page);
	} catch (Exception e) {
	  e.printStackTrace();
	  fail(e.getMessage());
	}
	assertNotNull(doc);
	assertEquals("assert title field only has 10 characters", 10, doc.getFieldValue("title").length());
  }
}
