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
package org.apache.nutch.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestIndexingFilters {

  /**
   * Test behaviour when defined filter does not exist.
   * 
   * @throws IndexingException
   */
  @Test
  public void testNonExistingIndexingFilter() throws IndexingException {
    Configuration conf = NutchConfiguration.create();
    conf.addResource("nutch-default.xml");
    conf.addResource("crawl-tests.xml");

    String class1 = "NonExistingFilter";
    String class2 = "org.apache.nutch.indexer.basic.BasicIndexingFilter";
    conf.set(IndexingFilters.INDEXINGFILTER_ORDER, class1 + " " + class2);

    IndexingFilters filters = new IndexingFilters(conf);
    filters.filter(new NutchDocument(), new ParseImpl("text", new ParseData(
        new ParseStatus(), "title", new Outlink[0], new Metadata())), new Text(
        "http://www.example.com/"), new CrawlDatum(), new Inlinks());
  }

  /**
   * Test behaviour when NutchDOcument is null
   */
  @Test
  public void testNutchDocumentNullIndexingFilter() throws IndexingException {
    Configuration conf = NutchConfiguration.create();
    conf.addResource("nutch-default.xml");
    conf.addResource("crawl-tests.xml");

    IndexingFilters filters = new IndexingFilters(conf);
    NutchDocument doc = filters.filter(null, new ParseImpl("text",
        new ParseData(new ParseStatus(), "title", new Outlink[0],
            new Metadata())), new Text("http://www.example.com/"),
        new CrawlDatum(), new Inlinks());

    assertNull(doc);
  }

  /**
   * Test behaviour when reset the index filter order will not take effect
   * 
   * @throws IndexingException
   */
  @Test
  public void testFilterCacheIndexingFilter() throws IndexingException {
    Configuration conf = NutchConfiguration.create();
    conf.addResource("nutch-default.xml");
    conf.addResource("crawl-tests.xml");

    String class1 = "org.apache.nutch.indexer.basic.BasicIndexingFilter";
    conf.set(IndexingFilters.INDEXINGFILTER_ORDER, class1);

    IndexingFilters filters1 = new IndexingFilters(conf);
    NutchDocument fdoc1 = filters1.filter(new NutchDocument(), new ParseImpl(
        "text", new ParseData(new ParseStatus(), "title", new Outlink[0],
            new Metadata())), new Text("http://www.example.com/"),
        new CrawlDatum(), new Inlinks());

    // add another index filter
    String class2 = "org.apache.nutch.indexer.metadata.MetadataIndexer";
    // set content metadata
    Metadata md = new Metadata();
    md.add("example", "data");
    // set content metadata property defined in MetadataIndexer
    conf.set("index.content.md", "example");
    // add MetadataIndxer filter
    conf.set(IndexingFilters.INDEXINGFILTER_ORDER, class1 + " " + class2);
    IndexingFilters filters2 = new IndexingFilters(conf);
    NutchDocument fdoc2 = filters2.filter(new NutchDocument(), new ParseImpl(
        "text", new ParseData(new ParseStatus(), "title", new Outlink[0], md)),
        new Text("http://www.example.com/"), new CrawlDatum(), new Inlinks());
    assertEquals(fdoc1.getFieldNames().size(), fdoc2.getFieldNames()
        .size());
  }

}
