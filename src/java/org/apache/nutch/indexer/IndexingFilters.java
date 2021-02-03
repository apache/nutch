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

import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.parse.Parse;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.hadoop.io.Text;

/** Creates and caches {@link IndexingFilter} implementing plugins. */
public class IndexingFilters {

  public static final String INDEXINGFILTER_ORDER = "indexingfilter.order";

  private IndexingFilter[] indexingFilters;

  public IndexingFilters(Configuration conf) {
    indexingFilters = (IndexingFilter[]) PluginRepository.get(conf)
        .getOrderedPlugins(IndexingFilter.class, IndexingFilter.X_POINT_ID,
            INDEXINGFILTER_ORDER);
  }

  /**
   * Run all defined filters. Note, may return null if the the document
   * was filtered
   * @see IndexingFilter#filter(NutchDocument, Parse, Text, CrawlDatum, Inlinks)
   * @param doc the {@link NutchDocument} to process with filters
   * @param parse corresponding {@link Parse} object for the document
   * @param url corresponding {@link org.apache.hadoop.io.Text} url for the document
   * @param datum corresponding {@link CrawlDatum} for the document
   * @param inlinks corresponding {@link Inlinks} for the document
   * @return the {@link NutchDocument}, null it the document was filtered
   * @throws IndexingException if an error occurs within a filter
   */
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {
    for (int i = 0; i < this.indexingFilters.length; i++) {
      doc = this.indexingFilters[i].filter(doc, parse, url, datum, inlinks);
      // break the loop if an indexing filter discards the doc
      if (doc == null)
        return null;
    }
    return doc;
  }

}
