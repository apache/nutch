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
package org.apache.nutch.indexer.anchor;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Indexing filter that offers an option to either index all inbound anchor text
 * for a document or deduplicate anchors. Deduplication does have it's con's,
 * 
 * @see {@code anchorIndexingFilter.deduplicate} in nutch-default.xml.
 */
public class AnchorIndexingFilter implements IndexingFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  private Configuration conf;
  private boolean deduplicate = false;

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration conf) {
    this.conf = conf;

    deduplicate = conf.getBoolean("anchorIndexingFilter.deduplicate", false);
    LOG.info("Anchor deduplication is: " + (deduplicate ? "on" : "off"));
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * The {@link AnchorIndexingFilter} filter object which supports boolean
   * configuration settings for the deduplication of anchors. See
   * {@code anchorIndexingFilter.deduplicate} in nutch-default.xml.
   * 
   * @param doc
   *          The {@link NutchDocument} object
   * @param parse
   *          The relevant {@link Parse} object passing through the filter
   * @param url
   *          URL to be filtered for anchor text
   * @param datum
   *          The {@link CrawlDatum} entry
   * @param inlinks
   *          The {@link Inlinks} containing anchor text
   * @return filtered NutchDocument
   */
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {

    String[] anchors = (inlinks != null ? inlinks.getAnchors() : new String[0]);

    HashSet<String> set = null;

    for (int i = 0; i < anchors.length; i++) {
      if (deduplicate) {
        if (set == null)
          set = new HashSet<String>();
        String lcAnchor = anchors[i].toLowerCase();

        // Check if already processed the current anchor
        if (!set.contains(lcAnchor)) {
          doc.add("anchor", anchors[i]);

          // Add to map
          set.add(lcAnchor);
        }
      } else {
        doc.add("anchor", anchors[i]);
      }
    }

    return doc;
  }

}
