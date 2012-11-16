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
package org.apache.nutch.indexer.anchor;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.util.NutchConfiguration;

/**
 * JUnit test case which tests
 * 1. that anchor text is obtained
 * 2. that anchor deduplication functionality is working
 * 
 * @author lewismc
 *
 */
public class TestAnchorIndexingFilter extends TestCase {

  public void testDeduplicateAnchor() throws Exception {
    Configuration conf = NutchConfiguration.create();
    conf.setBoolean("anchorIndexingFilter.deduplicate", true);
    AnchorIndexingFilter filter = new AnchorIndexingFilter();
    filter.setConf(conf);
    assertNotNull(filter);
    NutchDocument doc = new NutchDocument();
    ParseImpl parse = new ParseImpl("foo bar", new ParseData());
    Inlinks inlinks = new Inlinks();
    inlinks.add(new Inlink("http://test1.com/", "text1"));
    inlinks.add(new Inlink("http://test2.com/", "text2"));
    inlinks.add(new Inlink("http://test3.com/", "text2"));
    try {
      filter.filter(doc, parse, new Text("http://nutch.apache.org/index.html"), new CrawlDatum(), inlinks);
    } catch(Exception e){
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertNotNull(doc);
    assertTrue("test if there is an anchor at all", doc.getFieldNames().contains("anchor"));
    assertEquals("test dedup, we expect 2", 2, doc.getField("anchor").getValues().size());
  }

}
