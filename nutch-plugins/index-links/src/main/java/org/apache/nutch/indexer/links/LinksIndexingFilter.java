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
package org.apache.nutch.indexer.links;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * An {@link org.apache.nutch.indexer.IndexingFilter} that adds
 * <code>outlinks</code> and <code>inlinks</code> field(s) to the document.
 *
 * In case that you want to ignore the outlinks that point to the same host
 * as the URL being indexed use the following settings in your configuration
 * file:
 *
 * <property>
 *   <name>index.links.outlinks.host.ignore</name>
 *   <value>true</value>
 * </property>
 *
 * The same configuration is available for inlinks:
 *
 * <property>
 *   <name>index.links.inlinks.host.ignore</name>
 *   <value>true</value>
 * </property>
 *
 * To store only the host portion of each inlink URL or outlink URL add the
 * following to your configuration file.
 *
 * <property>
 *   <name>index.links.hosts.only</name>
 *   <value>false</value>
 * </property>
 *
 */
public class LinksIndexingFilter implements IndexingFilter {

  public final static String LINKS_OUTLINKS_HOST = "index.links.outlinks.host.ignore";
  public final static String LINKS_INLINKS_HOST = "index.links.inlinks.host.ignore";
  public final static String LINKS_ONLY_HOSTS = "index.links.hosts.only";

  public final static org.slf4j.Logger LOG = LoggerFactory
      .getLogger(LinksIndexingFilter.class);

  private Configuration conf;
  private boolean filterOutlinks;
  private boolean filterInlinks;
  private boolean indexHost;

  @Override
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {

    // Add the outlinks
    Outlink[] outlinks = parse.getData().getOutlinks();

    if (outlinks != null) {
      Set<String> hosts = new HashSet<String>();

      for (Outlink outlink : outlinks) {
        try {
          String linkUrl = outlink.getToUrl();
          String outHost = new URL(linkUrl).getHost().toLowerCase();

          if (indexHost) {
            linkUrl = outHost;

            if (hosts.contains(linkUrl))
              continue;

            hosts.add(linkUrl);
          }

          addFilteredLink("outlinks", url.toString(), linkUrl, outHost,
              filterOutlinks, doc);
        } catch (MalformedURLException e) {
          LOG.error("Malformed URL in {}: {}", url, e.getMessage());
        }
      }
    }

    // Add the inlinks
    if (null != inlinks) {
      Iterator<Inlink> iterator = inlinks.iterator();
      Set<String> inlinkHosts = new HashSet<String>();

      while (iterator.hasNext()) {
        try {
          Inlink link = iterator.next();
          String linkUrl = link.getFromUrl();
          String inHost = new URL(linkUrl).getHost().toLowerCase();

          if (indexHost) {
            linkUrl = inHost;

            if (inlinkHosts.contains(linkUrl))
              continue;

            inlinkHosts.add(linkUrl);
          }

          addFilteredLink("inlinks", url.toString(), linkUrl, inHost,
              filterInlinks, doc);
        } catch (MalformedURLException e) {
          LOG.error("Malformed URL in {}: {}", url, e.getMessage());
        }
      }
    }

    return doc;
  }

  private void addFilteredLink(String fieldName, String url, String linkUrl,
      String urlHost, boolean filter, NutchDocument doc) throws MalformedURLException {
      if (filter) {
        String host = new URL(url.toString()).getHost().toLowerCase();

        if (!host.equalsIgnoreCase(urlHost)) {
          doc.add(fieldName, linkUrl);
        }
      } else {
        doc.add(fieldName, linkUrl);
      }
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    filterOutlinks = conf.getBoolean(LINKS_OUTLINKS_HOST, false);
    filterInlinks = conf.getBoolean(LINKS_INLINKS_HOST, false);

    indexHost = conf.getBoolean(LINKS_ONLY_HOSTS, false);
  }

  public Configuration getConf() {
    return this.conf;
  }
}
