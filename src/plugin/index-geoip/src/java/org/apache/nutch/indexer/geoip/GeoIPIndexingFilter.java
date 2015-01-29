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
package org.apache.nutch.indexer.geoip;

import java.io.File;
import java.io.IOException;
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

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.WebServiceClient;

/**
 * <p>
 * This plugin implements an indexing filter which takes advantage of the <a
 * href="https://github.com/maxmind/GeoIP2-java">GeoIP2-java API</a>.
 * </p>
 * <p>
 * The third party library distribution provides an API for the GeoIP2 <a
 * href="http://dev.maxmind.com/geoip/geoip2/web-services">Precision web
 * services</a> and <a
 * href="http://dev.maxmind.com/geoip/geoip2/downloadable">databases</a>. The
 * API also works with the free <a
 * href="http://dev.maxmind.com/geoip/geoip2/geolite2/">GeoLite2 databases</a>.
 * </p>
 * <p>
 * Depending on the service level agreement, you have with the GeoIP service
 * provider, the plugin can add a number of the following fields to the index
 * data model:
 * <ol>
 * <li>Continent</li>
 * <li>Country</li>
 * <li>Regional Subdivision</li>
 * <li>City</li>
 * <li>Postal Code</li>
 * <li>Latitude/Longitude</li>
 * <li>ISP/Organization</li>
 * <li>AS Number</li>
 * <li>Confidence Factors</li>
 * <li>Radius</li>
 * <li>User Type</li>
 * </ol>
 * </p>
 * 
 * <p>
 * Some of the services are documented at the <a
 * href="https://www.maxmind.com/en/geoip2-precision-services">GeoIP2 Precision
 * Services</a> webpage where more information can be obtained.
 * </p>
 * 
 * <p>
 * You should also consult the following three properties in
 * <code>nutch-site.xml</code>
 * </p>
 * 
 * <pre>
 *  {@code
 * <!-- index-geoip plugin properties -->
 * <property>
 *   <name>index.geoip.usage</name>
 *   <value>insightsService</value>
 *   <description>
 *   A string representing the information source to be used for GeoIP information
 *   association. Either enter 'cityDatabase', 'connectionTypeDatabase', 
 *   'domainDatabase', 'ispDatabase' or 'insightsService'. If you wish to use any one of the 
 *   Database options, you should make one of GeoIP2-City.mmdb, GeoIP2-Connection-Type.mmdb, 
 *   GeoIP2-Domain.mmdb or GeoIP2-ISP.mmdb files respectively available on the Hadoop classpath 
 *   and available at runtime. This can be achieved by adding it to $NUTCH_HOME/conf
 *   </description>
 * </property>
 * 
 * <property>
 *   <name>index.geoip.userid</name>
 *   <value></value>
 *   <description>
 *   The userId associated with the GeoIP2 Precision Services account.
 *   </description>
 * </property>
 * 
 * <property>
 *   <name>index.geoip.licensekey</name>
 *   <value></value>
 *   <description>
 *   The license key associated with the GeoIP2 Precision Services account.
 *   </description>
 * </property>
 * }
 * </pre>
 * 
 */
public class GeoIPIndexingFilter implements IndexingFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(GeoIPIndexingFilter.class);

  private Configuration conf;

  private String usage = null;

  private File geoDb = null;

  WebServiceClient client = null;

  DatabaseReader reader = null;

  // private AbstractResponse response = null;

  /**
   * Default constructor for this plugin
   */
  public GeoIPIndexingFilter() {
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    String use = conf.get("index.geoip.usage", "insightsService");
    LOG.debug("GeoIP usage medium set to: {}", use);
    if (use.equalsIgnoreCase("cityDatabase")) {
      try {
        geoDb = new File(conf.getResource("GeoIP2-City.mmdb").getFile());
        buildDb();
      } catch (Exception e) {
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    } else if (use.equalsIgnoreCase("connectionTypeDatabase")) {
      try {
        geoDb = new File(conf.getResource("GeoIP2-Connection-Type.mmdb")
            .getFile());
        buildDb();
      } catch (Exception e) {
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    } else if (use.equalsIgnoreCase("domainDatabase")) {
      try {
        geoDb = new File(conf.getResource("GeoIP2-Domain.mmdb").getFile());
        buildDb();
      } catch (Exception e) {
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    } else if (use.equalsIgnoreCase("ispDatabase")) {
      try {
        geoDb = new File(conf.getResource("GeoIP2-ISP.mmdb").getFile());
        buildDb();
      } catch (Exception e) {
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    } else if (use.equalsIgnoreCase("insightsService")) {
      client = new WebServiceClient.Builder(conf.getInt("index.geoip.userid",
          12345), conf.get("index.geoip.licensekey")).build();
    }
    usage = use;
  }

  private void buildDb() {
    try {
      reader = new DatabaseReader.Builder(geoDb).build();
    } catch (IOException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  /**
   * 
   * @see org.apache.nutch.indexer.IndexingFilter#filter(org.apache.nutch.indexer.NutchDocument,
   *      org.apache.nutch.parse.Parse, org.apache.hadoop.io.Text,
   *      org.apache.nutch.crawl.CrawlDatum, org.apache.nutch.crawl.Inlinks)
   */
  @Override
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {
    return addServerGeo(doc, parse.getData(), url.toString());
  }

  private NutchDocument addServerGeo(NutchDocument doc, ParseData data,
      String url) {

    if (conf.getBoolean("store.ip.address", false) == true) {
      try {
        String serverIp = data.getContentMeta().get("_ip_");
        if (serverIp != null) {
          if (usage.equalsIgnoreCase("cityDatabase")) {
            doc = GeoIPDocumentCreator.createDocFromCityDb(serverIp, doc,
                reader);
          } else if (usage.equalsIgnoreCase("connectionTypeDatabase")) {
            doc = GeoIPDocumentCreator.createDocFromConnectionDb(serverIp, doc,
                reader);
          } else if (usage.equalsIgnoreCase("domainDatabase")) {
            doc = GeoIPDocumentCreator.createDocFromDomainDb(serverIp, doc,
                reader);
          } else if (usage.equalsIgnoreCase("ispDatabase")) {
            doc = GeoIPDocumentCreator
                .createDocFromIspDb(serverIp, doc, reader);
          } else if (usage.equalsIgnoreCase("insightsService")) {
            doc = GeoIPDocumentCreator.createDocFromInsightsService(serverIp,
                doc, client);
          }
        }
      } catch (Exception e) {
        LOG.error(e.getMessage());
        e.printStackTrace();
      }
    }
    return doc;
  }

}
