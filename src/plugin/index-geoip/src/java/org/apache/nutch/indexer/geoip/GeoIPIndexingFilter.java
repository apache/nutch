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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.EnumMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.WebServiceClient;
import com.maxmind.geoip2.exception.GeoIp2Exception;

/**
 * <p>This plugin implements an indexing filter which takes advantage of the <a
 * href="https://github.com/maxmind/GeoIP2-java">GeoIP2-java API</a>.</p>
 * <p>
 * The third party library distribution provides an API for the GeoIP2 <a
 * href="https://dev.maxmind.com/geoip/geolocate-an-ip/web-services">Precision web
 * services</a>, <a
 * href="https://dev.maxmind.com/geoip/geolite2-free-geolocation-data">
 * GeoLite2 (free)</a> and <a
 * href="https://dev.maxmind.com/geoip/geolocate-an-ip/databases">databases</a>.
 * </p>
 * <p>
 * Multiple databases can be configured simultaneously. Configure each database
 * type you want to use by setting its corresponding property:
 * </p>
 * <ul>
 *   <li><code>index.geoip.db.anonymous</code> - Anonymous IP database</li>
 *   <li><code>index.geoip.db.asn</code> - ASN database</li>
 *   <li><code>index.geoip.db.city</code> - City database</li>
 *   <li><code>index.geoip.db.connection</code> - Connection Type database</li>
 *   <li><code>index.geoip.db.country</code> - Country database</li>
 *   <li><code>index.geoip.db.domain</code> - Domain database</li>
 *   <li><code>index.geoip.db.isp</code> - ISP database</li>
 * </ul>
 * <p>
 * Alternatively, use the MaxMind Insights web service by setting
 * <code>index.geoip.insights.userid</code> and <code>index.geoip.insights.licensekey</code>.
 * </p>
 */
public class GeoIPIndexingFilter implements IndexingFilter, Closeable {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Supported GeoIP database types.
   */
  public enum DatabaseType {
    ANONYMOUS("anonymous"),
    ASN("asn"),
    CITY("city"),
    CONNECTION("connection"),
    COUNTRY("country"),
    DOMAIN("domain"),
    ISP("isp");

    private final String propertyName;

    DatabaseType(String propertyName) {
      this.propertyName = propertyName;
    }

    public String getPropertyName() {
      return propertyName;
    }
  }

  private static final String DB_PROPERTY_PREFIX = "index.geoip.db.";
  private static final String INSIGHTS_USERID = "index.geoip.insights.userid";
  private static final String INSIGHTS_LICENSEKEY = "index.geoip.insights.licensekey";

  private Configuration conf;
  private Map<DatabaseType, DatabaseReader> readers;
  private WebServiceClient insightsClient;

  /**
   * Default constructor for this plugin
   */
  public GeoIPIndexingFilter() {
    readers = new EnumMap<>(DatabaseType.class);
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration config) {
    conf = config;
    if (!config.getBoolean("store.ip.address", false)) {
      LOG.warn("Plugin index-geoip is active but IP address is not stored. "
          + "'store.ip.address' must be set to true in nutch-site.xml.");
    }

    // Initialize Insights web service if configured
    String userId = config.get(INSIGHTS_USERID);
    String licenseKey = config.get(INSIGHTS_LICENSEKEY);
    if (userId != null && !userId.isEmpty() && licenseKey != null && !licenseKey.isEmpty()) {
      insightsClient = new WebServiceClient.Builder(
          Integer.parseInt(userId), licenseKey).build();
      LOG.info("Initialized MaxMind Insights web service client.");
    }

    // Initialize database readers for each configured database type
    for (DatabaseType dbType : DatabaseType.values()) {
      String dbFile = config.get(DB_PROPERTY_PREFIX + dbType.getPropertyName());
      if (dbFile != null && !dbFile.isEmpty()) {
        loadDatabase(dbType, dbFile, config);
      }
    }

    if (readers.isEmpty() && insightsClient == null) {
      LOG.warn("No GeoIP databases or Insights service configured. "
          + "Set index.geoip.db.<type> properties or index.geoip.insights.* properties.");
    } else {
      LOG.debug("GeoIP plugin initialized with {} database(s){}",
          readers.size(),
          insightsClient != null ? " and Insights service" : "");
    }
  }

  /**
   * Load a GeoIP database from the classpath.
   * @param dbType the database type
   * @param dbFile the database file name
   * @param config the Hadoop configuration
   */
  private void loadDatabase(DatabaseType dbType, String dbFile, Configuration config) {
    InputStream db = config.getConfResourceAsInputStream(dbFile);
    if (db == null) {
      LOG.error("GeoIP {} database file '{}' not found on classpath", dbType, dbFile);
      return;
    }
    try {
      DatabaseReader reader = new DatabaseReader.Builder(db)
          .withCache(new CHMCache())
          .build();
      readers.put(dbType, reader);
      LOG.debug("Loaded GeoIP {} database from file: {}", dbType, dbFile);
    } catch (IOException e) {
      LOG.error("Failed to load GeoIP {} database from file: {} - {}",
          dbType, dbFile, e.getMessage());
    }
  }

  @Override
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {
    return augmentNutchDocWithIPData(doc, parse.getData());
  }

  private NutchDocument augmentNutchDocWithIPData(NutchDocument doc, ParseData data) {
    String serverIp = data.getContentMeta().get("_ip_");
    if (serverIp == null || serverIp.isEmpty()) {
      return doc;
    }

    if (readers.isEmpty() && insightsClient == null) {
      return doc;
    }

    // Query each configured database
    for (Map.Entry<DatabaseType, DatabaseReader> entry : readers.entrySet()) {
      try {
        doc = queryDatabase(entry.getKey(), serverIp, doc, entry.getValue());
      } catch (IOException | GeoIp2Exception e) {
        LOG.error("Error querying {} database for IP {}: {}",
            entry.getKey(), serverIp, e.getMessage());
      }
    }

    // Query Insights service if configured
    if (insightsClient != null) {
      try {
        doc = GeoIPDocumentCreator.createDocFromInsightsService(serverIp, doc, insightsClient);
      } catch (IOException | GeoIp2Exception e) {
        LOG.error("Error querying Insights service for IP {}: {}", serverIp, e.getMessage());
      }
    }

    return doc;
  }

  /**
   * Query the appropriate database based on type.
   */
  private NutchDocument queryDatabase(DatabaseType dbType, String serverIp,
      NutchDocument doc, DatabaseReader reader) throws IOException, GeoIp2Exception {
    switch (dbType) {
      case ANONYMOUS:
        return GeoIPDocumentCreator.createDocFromAnonymousIpDb(serverIp, doc, reader);
      case ASN:
        return GeoIPDocumentCreator.createDocFromAsnDb(serverIp, doc, reader);
      case CITY:
        return GeoIPDocumentCreator.createDocFromCityDb(serverIp, doc, reader);
      case CONNECTION:
        return GeoIPDocumentCreator.createDocFromConnectionDb(serverIp, doc, reader);
      case COUNTRY:
        return GeoIPDocumentCreator.createDocFromCountryDb(serverIp, doc, reader);
      case DOMAIN:
        return GeoIPDocumentCreator.createDocFromDomainDb(serverIp, doc, reader);
      case ISP:
        return GeoIPDocumentCreator.createDocFromIspDb(serverIp, doc, reader);
      default:
        LOG.warn("Unknown database type: {}", dbType);
        return doc;
    }
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<DatabaseType, DatabaseReader> entry : readers.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        LOG.warn("Error closing {} database reader: {}", entry.getKey(), e.getMessage());
      }
    }
    readers.clear();

    // WebServiceClient doesn't implement Closeable in GeoIP2 5.x
    insightsClient = null;
  }

}
