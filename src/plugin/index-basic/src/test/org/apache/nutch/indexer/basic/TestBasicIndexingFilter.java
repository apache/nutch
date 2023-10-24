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
package org.apache.nutch.indexer.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.basic.BasicIndexingFilter;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Date;

/**
 * JUnit test case which tests 1. that basic searchable fields are added to a
 * document 2. that domain is added as per {@code indexer.add.domain} in
 * nutch-default.xml. 3. that title is truncated as per
 * {@code indexer.max.title.length} in nutch-default.xml. 4. that content is
 * truncated as per {@code indexer.max.content.length} in nutch-default.xml.
 * 
 * @author tejasp
 * 
 */

public class TestBasicIndexingFilter {

  @Test
  public void testBasicIndexingFilter() throws Exception {
    Configuration conf = NutchConfiguration.create();
    conf.setInt("indexer.max.title.length", 10);
    conf.setBoolean("indexer.add.domain", true);
    conf.setInt("indexer.max.content.length", 20);

    BasicIndexingFilter filter = new BasicIndexingFilter();
    filter.setConf(conf);
    Assertions.assertNotNull(filter);

    NutchDocument doc = new NutchDocument();

    String title = "The Foo Page";
    Outlink[] outlinks = new Outlink[] { new Outlink("http://foo.com/", "Foo") };
    Metadata metaData = new Metadata();
    metaData.add("Language", "en/us");
    ParseData parseData = new ParseData(ParseStatus.STATUS_SUCCESS, title,
        outlinks, metaData);
    ParseImpl parse = new ParseImpl(
        "this is a sample foo bar page. hope you enjoy it.", parseData);

    CrawlDatum crawlDatum = new CrawlDatum();
    crawlDatum.setFetchTime(100L);

    Inlinks inlinks = new Inlinks();

    try {
      filter.filter(doc, parse, new Text("http://nutch.apache.org/index.html"),
          crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      Assertions.fail(e.getMessage());
    }
    Assertions.assertNotNull(doc);
    Assertions.assertEquals("The Foo Pa",
        doc.getField("title").getValues().get(0),
        "test title, expect \"The Foo Pa\"");
    Assertions.assertEquals("apache.org",
        doc.getField("domain").getValues().get(0),
        "test domain, expect \"apache.org\"");
    Assertions.assertEquals("nutch.apache.org",
        doc.getField("host").getValues().get(0),
        "test host, expect \"nutch.apache.org\"");
    Assertions.assertEquals("http://nutch.apache.org/index.html",
        doc.getField("url").getValues().get(0),
        "test url, expect \"http://nutch.apache.org/index.html\"");
    Assertions.assertEquals(doc.getField("content").getValues().get(0),
        "test content", "this is a sample foo");
    Assertions.assertEquals(new Date(100L),
        (Date) doc.getField("tstamp").getValues().get(0),
        "test fetch time");
  }
}
