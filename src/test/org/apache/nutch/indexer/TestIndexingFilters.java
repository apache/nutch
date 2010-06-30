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

import junit.framework.TestCase;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;

public class TestIndexingFilters extends TestCase {

  /**
   * Test behaviour when defined filter does not exist.
   * @throws IndexingException
   */
  public void testNonExistingIndexingFilter() throws IndexingException {
    Configuration conf = NutchConfiguration.create();
    String class1 = "NonExistingFilter";
    String class2 = "org.apache.nutch.indexer.basic.BasicIndexingFilter";
    conf.set(IndexingFilters.INDEXINGFILTER_ORDER, class1 + " " + class2);

    IndexingFilters filters = new IndexingFilters(conf);
//    filters.filter(new NutchDocument(), new ParseImpl("text", new ParseData(
//        new ParseStatus(), "title", new Outlink[0], new Metadata())), new Text(
//        "http://www.example.com/"), new CrawlDatum(), new Inlinks());
    WebPage page = new WebPage();
    page.setText(new Utf8("text"));
    page.setTitle(new Utf8("title"));
    filters.filter(new NutchDocument(),"http://www.example.com/",page);
  }

}
