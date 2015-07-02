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
package org.apache.nutch.indexer.staticfield;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * JUnit test case which tests 1. that static data fields are added to a
 * document 2. that empty {@code index.static} does not add anything to the
 * document 3. that valid field:value pairs are added to the document 4. that
 * fields and values added to the document are trimmed
 * 
 * @author tejasp
 */

public class TestStaticFieldIndexerTest {

  Configuration conf;

  Inlinks inlinks;
  ParseImpl parse;
  CrawlDatum crawlDatum;
  Text url;
  StaticFieldIndexer filter;

  @Before
  public void setUp() throws Exception {
    conf = NutchConfiguration.create();
    parse = new ParseImpl();
    url = new Text("http://nutch.apache.org/index.html");
    crawlDatum = new CrawlDatum();
    inlinks = new Inlinks();
    filter = new StaticFieldIndexer();
  }

  /**
   * Test that empty {@code index.static} does not add anything to the document
   * 
   * @throws Exception
   */
  @Test
  public void testEmptyIndexStatic() throws Exception {

    Assert.assertNotNull(filter);
    filter.setConf(conf);

    NutchDocument doc = new NutchDocument();

    try {
      filter.filter(doc, parse, url, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    Assert.assertNotNull(doc);
    Assert.assertTrue("tests if no field is set for empty index.static", doc
        .getFieldNames().isEmpty());
  }

  /**
   * Test that valid field:value pairs are added to the document
   * 
   * @throws Exception
   */
  @Test
  public void testNormalScenario() throws Exception {

    conf.set("index.static",
        "field1:val1, field2    :      val2 val3     , field3, field4 :val4 , ");
    Assert.assertNotNull(filter);
    filter.setConf(conf);

    NutchDocument doc = new NutchDocument();

    try {
      filter.filter(doc, parse, url, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    Assert.assertNotNull(doc);
    Assert.assertFalse("test if doc is not empty", doc.getFieldNames()
        .isEmpty());
    Assert.assertEquals("test if doc has 3 fields", 3, doc.getFieldNames()
        .size());
    Assert.assertTrue("test if doc has field1", doc.getField("field1")
        .getValues().contains("val1"));
    Assert.assertTrue("test if doc has field2", doc.getField("field2")
        .getValues().contains("val2"));
    Assert.assertTrue("test if doc has field4", doc.getField("field4")
        .getValues().contains("val4"));
  }

  /**
   * Test for NUTCH-2052 custom delimiters in index.static.
   *
   * @throws Exception
   */
  @Test
  public void testCustomDelimiters() throws Exception {

    conf.set("index.static.fieldsep", ">");
    conf.set("index.static.keysep", "=");
    conf.set("index.static.valuesep", "|");
    conf.set("index.static",
        "field1=val1>field2    =      val2|val3     >field3>field4 =val4 > ");
    Assert.assertNotNull(filter);
    filter.setConf(conf);

    NutchDocument doc = new NutchDocument();

    try {
      filter.filter(doc, parse, url, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    Assert.assertNotNull(doc);
    Assert.assertFalse("test if doc is not empty", doc.getFieldNames()
        .isEmpty());
    Assert.assertEquals("test if doc has 3 fields", 3, doc.getFieldNames()
        .size());
    Assert.assertTrue("test if doc has field1", doc.getField("field1")
        .getValues().contains("val1"));
    Assert.assertTrue("test if doc has field2", doc.getField("field2")
        .getValues().contains("val2"));
    Assert.assertTrue("test if doc has field4", doc.getField("field4")
        .getValues().contains("val4"));
  }

  /**
   * Test for NUTCH-2052 custom delimiters in index.static.
   *
   * @throws Exception
   */
  @Test
  public void testCustomMulticharacterDelimiters() throws Exception {

    conf.set("index.static.fieldsep", "\n\n");
    conf.set("index.static.keysep", "\t\t");
    conf.set("index.static.valuesep", "***");
    conf.set("index.static",
      "field1\t\tval1\n\n" + 
      "field2\t\tval2***val3\n\n" + 
      "field3\n\n" +
      "field4\t\tval4\n\n\n\n");
    Assert.assertNotNull(filter);
    filter.setConf(conf);

    NutchDocument doc = new NutchDocument();

    try {
      filter.filter(doc, parse, url, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    Assert.assertNotNull(doc);
    Assert.assertFalse("test if doc is not empty", doc.getFieldNames()
        .isEmpty());
    Assert.assertEquals("test if doc has 3 fields", 3, doc.getFieldNames()
        .size());
    Assert.assertTrue("test if doc has field1", doc.getField("field1")
        .getValues().contains("val1"));
    Assert.assertTrue("test if doc has field2", doc.getField("field2")
        .getValues().contains("val2"));
    Assert.assertTrue("test if doc has field4", doc.getField("field4")
        .getValues().contains("val4"));
  }
}
