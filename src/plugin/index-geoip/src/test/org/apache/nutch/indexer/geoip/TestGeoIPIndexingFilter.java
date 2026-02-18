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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

  @BeforeEach
  public void setUp() {
    conf = NutchConfiguration.create();
    conf.setBoolean("store.ip.address", true);
    doc = new NutchDocument();
    parseImpl = new ParseImpl("foo bar", new ParseData());
    inlinks = new Inlinks();
    text = new Text("http://nutch.apache.org/index.html");
    crawlDatum = new CrawlDatum();
  }

  @AfterEach
  public void teardown() throws Exception {
    if (filter != null) {
      filter.close();
    }
  }

  /**
   * Test method for {@link org.apache.nutch.indexer.geoip.GeoIPIndexingFilter#getConf()}.
   */
  @Test
  public void testGetConf() {
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    assertTrue(filter.getConf().getBoolean("store.ip.address", true));
  }

  /**
   * Test that City database configuration property works correctly.
   */
  @Test
  public void testSetConfCityDbProperty() {
    conf.set("index.geoip.db.city", "GeoIP2-City-Test.mmdb");
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    assertEquals("GeoIP2-City-Test.mmdb", filter.getConf().get("index.geoip.db.city"));
  }

  /**
   * Test City database with Singapore location data.
   */
  @Test
  public void testCityDatabaseSingapore() {
    conf.set("index.geoip.db.city", "GeoIP2-City-Test.mmdb");
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    // IP 214.0.0.1 maps to Singapore in the test database
    parseImpl.getData().getContentMeta().add("_ip_", "214.0.0.1");
    try {
      filter.filter(doc, parseImpl, text, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertNotNull(doc);
    // Verify city data for Singapore
    assertEquals("Singapore", doc.getFieldValue("cityName"));
    assertEquals("AS", doc.getFieldValue("continentCode"));
    assertEquals("Asia", doc.getFieldValue("continentName"));
    assertEquals("SG", doc.getFieldValue("countryIsoCode"));
    assertEquals("Singapore", doc.getFieldValue("countryName"));
    assertEquals("Asia/Singapore", doc.getFieldValue("timeZone"));
    assertEquals("59", doc.getFieldValue("postalCode"));
    // Verify lat/lon is present
    assertNotNull(doc.getFieldValue("latLon"));
    assertTrue(doc.getFieldValue("latLon").toString().contains("1.336"));
  }

  /**
   * Test City database with Melbourne, Australia location data.
   */
  @Test
  public void testCityDatabaseMelbourne() {
    conf.set("index.geoip.db.city", "GeoIP2-City-Test.mmdb");
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    // IP 214.0.1.1 maps to Melbourne, Australia in the test database
    parseImpl.getData().getContentMeta().add("_ip_", "214.0.1.1");
    try {
      filter.filter(doc, parseImpl, text, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertNotNull(doc);
    // Verify city data for Melbourne
    assertEquals("Melbourne", doc.getFieldValue("cityName"));
    assertEquals("OC", doc.getFieldValue("continentCode"));
    assertEquals("Oceania", doc.getFieldValue("continentName"));
    assertEquals("AU", doc.getFieldValue("countryIsoCode"));
    assertEquals("Australia", doc.getFieldValue("countryName"));
    assertEquals("Australia/Melbourne", doc.getFieldValue("timeZone"));
    // Verify lat/lon is present
    assertNotNull(doc.getFieldValue("latLon"));
    assertTrue(doc.getFieldValue("latLon").toString().contains("-37.8159"));
  }

  /**
   * Test Connection Type database with Cable/DSL connection.
   */
  @Test
  public void testConnectionTypeCableDsl() {
    conf.set("index.geoip.db.connection", "GeoIP2-Connection-Type-Test.mmdb");
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    // IP 1.0.0.1 maps to Cable/DSL connection type in the test database
    parseImpl.getData().getContentMeta().add("_ip_", "1.0.0.1");
    try {
      filter.filter(doc, parseImpl, text, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertNotNull(doc);
    assertEquals("Cable/DSL", doc.getFieldValue("connectionType"));
    assertNotNull(doc.getFieldValue("ip"));
    assertNotNull(doc.getFieldValue(GeoIPDocumentCreator.CONNECTION_NETWORK_ADDRESS));
  }

  /**
   * Test Connection Type database with Cellular connection.
   */
  @Test
  public void testConnectionTypeCellular() {
    conf.set("index.geoip.db.connection", "GeoIP2-Connection-Type-Test.mmdb");
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    // IP 1.0.1.1 maps to Cellular connection type in the test database
    parseImpl.getData().getContentMeta().add("_ip_", "1.0.1.1");
    try {
      filter.filter(doc, parseImpl, text, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertNotNull(doc);
    assertEquals("Cellular", doc.getFieldValue("connectionType"));
    assertNotNull(doc.getFieldValue("ip"));
  }

  /**
   * Test Connection Type database with Corporate connection.
   */
  @Test
  public void testConnectionTypeCorporate() {
    conf.set("index.geoip.db.connection", "GeoIP2-Connection-Type-Test.mmdb");
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    // IP 201.243.200.1 maps to Corporate connection type in the test database
    parseImpl.getData().getContentMeta().add("_ip_", "201.243.200.1");
    try {
      filter.filter(doc, parseImpl, text, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertNotNull(doc);
    assertEquals("Corporate", doc.getFieldValue("connectionType"));
    assertNotNull(doc.getFieldValue("ip"));
  }

  /**
   * Test multiple databases configured simultaneously.
   * Both City and Connection Type databases are loaded and queried.
   */
  @Test
  public void testMultipleDatabases() {
    // Configure both City and Connection Type databases
    conf.set("index.geoip.db.city", "GeoIP2-City-Test.mmdb");
    conf.set("index.geoip.db.connection", "GeoIP2-Connection-Type-Test.mmdb");
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    
    // Use IP 1.0.0.1 which exists in Connection Type test database
    // Note: This IP may not exist in the City database, so we just verify
    // that Connection Type data is returned
    parseImpl.getData().getContentMeta().add("_ip_", "1.0.0.1");
    try {
      filter.filter(doc, parseImpl, text, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertNotNull(doc);
    // Verify connection type data is present
    assertEquals("Cable/DSL", doc.getFieldValue("connectionType"));
    assertNotNull(doc.getFieldValue("ip"));
  }

  /**
   * Test that filter handles null/empty IP gracefully.
   */
  @Test
  public void testNullIpAddress() {
    conf.set("index.geoip.db.city", "GeoIP2-City-Test.mmdb");
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    // Don't set any IP address
    try {
      NutchDocument result = filter.filter(doc, parseImpl, text, crawlDatum, inlinks);
      assertNotNull(result);
      // Document should be returned unchanged
      assertTrue(result.getFieldNames().isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * Test that filter handles no configured databases gracefully.
   */
  @Test
  public void testNoConfiguredDatabases() {
    // Don't configure any databases
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    parseImpl.getData().getContentMeta().add("_ip_", "1.0.0.1");
    try {
      NutchDocument result = filter.filter(doc, parseImpl, text, crawlDatum, inlinks);
      assertNotNull(result);
      // Document should be returned unchanged when no databases configured
      assertTrue(result.getFieldNames().isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  /**
   * Test addIfNotDuplicate method adds value when field doesn't exist.
   */
  @Test
  public void testAddIfNotDuplicateNewField() {
    NutchDocument testDoc = new NutchDocument();
    GeoIPDocumentCreator.addIfNotDuplicate(testDoc, "ip", "192.168.1.1");
    
    assertNotNull(testDoc.getFieldValue("ip"));
    assertEquals("192.168.1.1", testDoc.getFieldValue("ip"));
    assertEquals(1, testDoc.getField("ip").getValues().size());
  }

  /**
   * Test addIfNotDuplicate method doesn't add duplicate values.
   */
  @Test
  public void testAddIfNotDuplicatePreventsDuplicates() {
    NutchDocument testDoc = new NutchDocument();
    
    // Add the same IP three times (simulating multiple database lookups)
    GeoIPDocumentCreator.addIfNotDuplicate(testDoc, "ip", "192.168.1.1");
    GeoIPDocumentCreator.addIfNotDuplicate(testDoc, "ip", "192.168.1.1");
    GeoIPDocumentCreator.addIfNotDuplicate(testDoc, "ip", "192.168.1.1");
    
    // Should only have one value
    NutchField field = testDoc.getField("ip");
    assertNotNull(field);
    assertEquals(1, field.getValues().size());
    assertEquals("192.168.1.1", field.getValues().get(0));
  }

  /**
   * Test addIfNotDuplicate method allows different values.
   */
  @Test
  public void testAddIfNotDuplicateAllowsDifferentValues() {
    NutchDocument testDoc = new NutchDocument();
    
    // Add different IPs (this shouldn't happen in practice but tests the logic)
    GeoIPDocumentCreator.addIfNotDuplicate(testDoc, "ip", "192.168.1.1");
    GeoIPDocumentCreator.addIfNotDuplicate(testDoc, "ip", "192.168.1.2");
    
    // Should have two different values
    NutchField field = testDoc.getField("ip");
    assertNotNull(field);
    assertEquals(2, field.getValues().size());
    assertTrue(field.getValues().contains("192.168.1.1"));
    assertTrue(field.getValues().contains("192.168.1.2"));
  }

  /**
   * Test addIfNotDuplicate method handles null values gracefully.
   */
  @Test
  public void testAddIfNotDuplicateNullValue() {
    NutchDocument testDoc = new NutchDocument();
    
    // Adding null should not create a field
    GeoIPDocumentCreator.addIfNotDuplicate(testDoc, "ip", null);
    
    assertNull(testDoc.getField("ip"));
  }

  /**
   * Test that database-specific network address field names are used correctly.
   * Connection Type database should use connectionNetworkAddress field.
   */
  @Test
  public void testConnectionDatabaseSpecificNetworkAddressField() {
    conf.set("index.geoip.db.connection", "GeoIP2-Connection-Type-Test.mmdb");
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    parseImpl.getData().getContentMeta().add("_ip_", "1.0.0.1");
    try {
      filter.filter(doc, parseImpl, text, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertNotNull(doc);
    // Verify that database-specific network address field is used
    assertNotNull(doc.getFieldValue(GeoIPDocumentCreator.CONNECTION_NETWORK_ADDRESS));
    // Verify that the old generic field is NOT used
    assertNull(doc.getField("networkAddress"));
  }

  /**
   * Test that multiple databases use their own specific network address fields
   * and that IP address is not duplicated.
   */
  @Test
  public void testMultipleDatabasesUseSpecificNetworkAddressFields() {
    // Configure both City and Connection Type databases
    conf.set("index.geoip.db.city", "GeoIP2-City-Test.mmdb");
    conf.set("index.geoip.db.connection", "GeoIP2-Connection-Type-Test.mmdb");
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    
    // Use IP 1.0.0.1 which exists in Connection Type test database
    parseImpl.getData().getContentMeta().add("_ip_", "1.0.0.1");
    try {
      filter.filter(doc, parseImpl, text, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    assertNotNull(doc);
    
    // Verify connection type data is present with specific network address field
    assertEquals("Cable/DSL", doc.getFieldValue("connectionType"));
    assertNotNull(doc.getFieldValue(GeoIPDocumentCreator.CONNECTION_NETWORK_ADDRESS));
    
    // Verify the generic networkAddress field is NOT used
    assertNull(doc.getField("networkAddress"));
    
    // Verify IP field exists and has only one value (not duplicated)
    NutchField ipField = doc.getField("ip");
    assertNotNull(ipField);
    assertEquals(1, ipField.getValues().size(), 
        "IP field should have exactly one value, not duplicated across databases");
  }

  /**
   * Test that Country database configuration property is correctly recognized.
   * Note: This test verifies the configuration works. A full test would require
   * a GeoIP2-Country-Test.mmdb file which is not currently available in the
   * test resources. The City database can be used for country-level lookups
   * in production as it contains the same country data.
   */
  @Test
  public void testCountryDatabaseConfigProperty() {
    // Verify the country property is recognized (won't load without actual file)
    conf.set("index.geoip.db.country", "nonexistent-country-test.mmdb");
    filter = new GeoIPIndexingFilter();
    filter.setConf(conf);
    // Configuration should be accepted without error (file missing is logged as warning)
    assertEquals("nonexistent-country-test.mmdb", 
        filter.getConf().get("index.geoip.db.country"));
  }

  /**
   * Test Country database specific network address field constant is defined.
   */
  @Test
  public void testCountryNetworkAddressFieldConstant() {
    // Verify the COUNTRY_NETWORK_ADDRESS constant is properly defined
    assertEquals("countryNetworkAddress", GeoIPDocumentCreator.COUNTRY_NETWORK_ADDRESS);
  }
}
