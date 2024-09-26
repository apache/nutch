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
package org.apache.nutch.indexer.geoip;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.annotation.processing.Filer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link org.apache.nutch.indexer.geoip.GeoIPIndexingFilter}
 */
public class TestGeoIPIndexingFilter {

  private Configuration conf;
  private GeoIPIndexingFilter filter;
  private NutchDocument doc;
  private ParseImpl parseImpl;
  private Text text;
  private CrawlDatum crawlDatum;
  private Inlinks inlinks;

  @Before
  public void setUp() {
    conf = NutchConfiguration.create();
    conf.setBoolean("store.ip.address", true);
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    doc = new NutchDocument();
    parseImpl = new ParseImpl("foo bar", new ParseData());
    inlinks = new Inlinks();
    text = new Text("http://nutch.apache.org/index.html");
    crawlDatum = new CrawlDatum();
  }

  @After
  public void teardown() {
    filter.getConf().clear();
  }

  /**
   * Test method for {@link org.apache.nutch.indexer.geoip.GeoIPIndexingFilter#getConf()}.
   */
  @Test
  public final void testGetConf() {
    assertTrue(filter.getConf().getBoolean("store.ip.address", true));
  }

  /**
   * Test method for {@link org.apache.nutch.indexer.geoip.GeoIPIndexingFilter#setConf(org.apache.hadoop.conf.Configuration)}.
   */
  @Test
  public final void testSetConfCaseInsensitive() {
    assertNull(filter.getConf().get("index.geoip.usage"));
    // test for case insensitivity
    filter.getConf().set("index.geoip.usage", "InSiGhTs");
    assertTrue(filter.getConf().get("index.geoip.usage").equalsIgnoreCase("insights"));
  }

  /**
   * Test method for {@link org.apache.nutch.indexer.geoip.GeoIPIndexingFilter#setConf(org.apache.hadoop.conf.Configuration)}.
   */
  @Test
  public final void testSetConfDbFile() {
    assertNull(filter.getConf().get("index.geoip.db.file"));
    // test for case insensitivity
    filter.getConf().set("index.geoip.usage", "CiTy");
    filter.getConf().set("index.geoip.db.file", "GeoIP2-City-Test.mmdb");
    assertEquals(filter.getConf().get("index.geoip.db.file"), "GeoIP2-City-Test.mmdb");
  }

  /**
   * Test method for {@link org.apache.nutch.indexer.geoip.GeoIPIndexingFilter#filter(org.apache.nutch.indexer.NutchDocument, org.apache.nutch.parse.Parse, org.apache.hadoop.io.Text, org.apache.nutch.crawl.CrawlDatum, org.apache.nutch.crawl.Inlinks)}.
   * Uses the GeoIP2 Anonymous IP database to augment NutchDocument fields.
   * @throws IndexingException 
   */
  @Test
  public void testAnonymousIPDatabaseGeoIPIndexingFilter() {
    conf.set("index.geoip.usage", "anonymous");
    conf.set("index.geoip.db.file", "GeoIP2-Anonymous-IP-Test.mmdb");
    filter.setConf(conf);
    parseImpl.getData().getContentMeta().add("_ip_", "::81.2.69.0/120");
    try {
      filter.filter(doc, parseImpl, text, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertNotNull(doc);
    System.out.print(doc.toString());
    assertNotNull(doc.getField("is_anonymous"));
    assertNotEquals(doc.getFieldValue("is_tor_exit_node"), "true");
    assertEquals(10, doc.getFieldNames().size());
    assertTrue("NutchDocument contains 'is_public_proxy' field.", 
        doc.getFieldNames().contains("is_public_proxy"));
  }
//
//  @Test
//  public void testAsnIPDatabaseGeoIPIndexingFilter() {
//    conf.set("index.geoip.usage", "asn");
//    conf.set("index.geoip.db.file", "GeoLite2-ASN-Test.mmdb");
//
//    GeoIPIndexingFilter filter = new GeoIPIndexingFilter();
//    filter.setConf(conf);
//    Assert.assertNotNull(filter);
//
//    NutchDocument doc = new NutchDocument();
//  }
//  
//  @Test
//  public void testCityDatabaseGeoIPIndexingFilter() {
//    conf.set("index.geoip.usage", "city");
//    conf.set("index.geoip.db.file", "GeoIP2-City-Test.mmdb");
//
//    GeoIPIndexingFilter filter = new GeoIPIndexingFilter();
//    filter.setConf(conf);
//    Assert.assertNotNull(filter);
//
//    NutchDocument doc = new NutchDocument();
//  }
//  
//  @Test
//  public void testConnectionDatabaseGeoIPIndexingFilter() {
//    conf.set("index.geoip.usage", "connection");
//    conf.set("index.geoip.db.file", "GeoIP2-Connection-Type-Test.mmdb");
//
//    GeoIPIndexingFilter filter = new GeoIPIndexingFilter();
//    filter.setConf(conf);
//    Assert.assertNotNull(filter);
//
//    NutchDocument doc = new NutchDocument();
//  }
//  
//  @Test
//  public void testDomainDatabaseGeoIPIndexingFilter() {
//    conf.set("index.geoip.usage", "domain");
//    conf.set("index.geoip.db.file", "GeoIP2-Domain-Test.mmdb");
//
//    GeoIPIndexingFilter filter = new GeoIPIndexingFilter();
//    filter.setConf(conf);
//    Assert.assertNotNull(filter);
//
//    NutchDocument doc = new NutchDocument();
//  }
//  
//  @Test
//  public void testInsightsGeoIPIndexingFilter() {
//    conf.set("index.geoip.usage", "insights");
//    conf.set("index.geoip.userid", "");
//    conf.set("index.geoip.licensekey", "");
//
//    GeoIPIndexingFilter filter = new GeoIPIndexingFilter();
//    filter.setConf(conf);
//    Assert.assertNotNull(filter);
//
//    NutchDocument doc = new NutchDocument();
//  }
//  
//  @Test
//  public void testIspDatabaseGeoIPIndexingFilter() {
//    conf.set("index.geoip.usage", "isp");
//    conf.set("index.geoip.db.file", "GeoIP2-ISP-Test.mmdb");
//
//    GeoIPIndexingFilter filter = new GeoIPIndexingFilter();
//    filter.setConf(conf);
//    Assert.assertNotNull(filter);
//
//    NutchDocument doc = new NutchDocument();
//  }
}
