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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.Objects;

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
 * See the <a
 * href="https://www.maxmind.com/en/geoip2-precision-services">GeoIP2 Precision
 * Services</a> webpage for more information.
 * </p>
 * <p>
 * You should consult and configure the <b>index.geoip.*</b> properties in 
 * <code>nutch-site.xml</code>.
 * </p>
 */
public class GeoIPIndexingFilter implements IndexingFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  private Configuration conf;
  private String usage;
  private static final String INSIGHTS_SERVICE = "insights";
  private WebServiceClient client;
  private DatabaseReader reader;

  /**
   * Default constructor for this plugin
   */
  public GeoIPIndexingFilter() {
    //Constructor
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Set plugin {@link org.apache.hadoop.conf.Configuration}
   * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration config) {
    conf = config;
    if (!config.getBoolean("store.ip.address", false)) {
      LOG.warn("Plugin index-geoip is active but IP address is not stored. "
          + "'store.ip.address' must be set to true in nutch-site.xml.");
    }
    usage = config.get("index.geoip.usage");
    if (usage != null && usage.equalsIgnoreCase(INSIGHTS_SERVICE)) {
      client = new WebServiceClient.Builder(
              Integer.parseInt(config.get("index.geoip.userid")),
              config.get("index.geoip.licensekey")).build();
      LOG.debug("Established geoip-index InsightsService client.");
    } else if (usage != null && !usage.equalsIgnoreCase(INSIGHTS_SERVICE)) {
      String dbFile = config.get("index.geoip.db.file");
      if (dbFile != null) {
        LOG.debug("GeoIP db file: {}", dbFile);
        URL dbFileUrl = config.getResource(dbFile);
        if (dbFileUrl == null) {
          LOG.error("Db file {} not found on classpath", dbFile);
        } else {
          try {
            buildDb(new File(dbFileUrl.getFile()));
          } catch (Exception e) {
            LOG.error("Failed to read Db file: {} {}", dbFile, e.getMessage());
          }
        }
      }
    }
  }

  /*
   * Build the Database and
   * <a href="https://github.com/maxmind/GeoIP2-java/tree/main?tab=readme-ov-file#caching">
   * associated cache</a>.
   * @param geoDb the GeoIP2 database to be used for IP lookups.
   */
  private void buildDb(File geoDb) {
    try {
      LOG.info("Reading index-geoip Db file: {}", geoDb);
      reader = Objects.requireNonNull(new DatabaseReader.Builder(geoDb).withCache(new CHMCache()).build());
    } catch (IOException | NullPointerException e) {
      LOG.error("Failed to build Db: {}", e.getMessage());
    }
  }

  /**
   * Filter the document.
   * @return A {@link org.apache.nutch.indexer.NutchDocument} with added geoip fields.
   * @see org.apache.nutch.indexer.IndexingFilter#filter(org.apache.nutch.indexer.NutchDocument,
   *      org.apache.nutch.parse.Parse, org.apache.hadoop.io.Text,
   *      org.apache.nutch.crawl.CrawlDatum, org.apache.nutch.crawl.Inlinks)
   */
  @Override
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {
    return augmentNutchDocWithIPData(doc, parse.getData());
  }

  private NutchDocument augmentNutchDocWithIPData(NutchDocument doc, ParseData data) {
    String serverIp = data.getContentMeta().get("_ip_");
    // The global DatabaseReader variable is already NonNull so no null check required.
    if (!serverIp.isEmpty()) {
      try {
        switch (conf.get("index.geoip.usage").toLowerCase()) {
        case "anonymous":
          doc = GeoIPDocumentCreator.createDocFromAnonymousIpDb(serverIp, doc, reader);
          break;
        case "asn":
          doc = GeoIPDocumentCreator.createDocFromAsnDb(serverIp, doc, reader);
          break;
        case "city":
          doc = GeoIPDocumentCreator.createDocFromCityDb(serverIp, doc, reader);
          break;
        case "connection":
          doc = GeoIPDocumentCreator.createDocFromConnectionDb(serverIp, doc, reader);
          break;
        case "domain":
          doc = GeoIPDocumentCreator.createDocFromDomainDb(serverIp, doc, reader);
          break;
        case INSIGHTS_SERVICE:
          doc = GeoIPDocumentCreator.createDocFromInsightsService(serverIp, doc, client);
          break;
        case "isp":
          doc = GeoIPDocumentCreator.createDocFromIspDb(serverIp, doc, reader);
          break;
        default:
          LOG.error("Failed to determine 'index.geoip.usage' value: {}", usage);
        }
      } catch (IOException | GeoIp2Exception e) {
          LOG.error("Error creating index-geoip fields _ip_: {}, databe type: {} \n{}",
              serverIp, reader.getMetadata().getDatabaseType(), e.getMessage());
      }
    }
    return doc;
  }

}
