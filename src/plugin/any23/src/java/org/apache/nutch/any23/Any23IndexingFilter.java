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
package org.apache.nutch.any23;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;

/**
 * <p>This implementation of {@link org.apache.nutch.indexer.IndexingFilter}
 * adds a <i>triple(s)</i> field to the {@link org.apache.nutch.indexer.NutchDocument}.</p>
 * <p>Triples are extracted via <a href="http://any23.apache.org">Apache Any23</a>.</p>
 * @see {@link org.apache.nutch.any23.Any23ParseFilter}.
 */
public class Any23IndexingFilter implements IndexingFilter {

  private Configuration conf;

  /**
   * Get the {@link Configuration} object
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Set the {@link Configuration} object
   * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   *
   * @param doc
   *          document instance for collecting fields
   * @param parse
   *          parse data instance
   * @param url
   *          page url
   * @param datum
   *          crawl datum for the page (fetch datum from segment containing
   *          fetch status and fetch time)
   * @param inlinks
   *          page inlinks
   * @return filtered NutchDocument
   * @see org.apache.nutch.indexer.IndexingFilter#filter(NutchDocument, Parse, Text, CrawlDatum, Inlinks)
   *
   * @throws IndexingException
   */
  @Override
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks) throws IndexingException {
    // Check if some Triples are found, possibly put there by Any23ParseFilter
    String[] metadata = parse.getData().getParseMeta().getValues(Any23ParseFilter.ANY23_TRIPLES);

    if (metadata != null && metadata.length >= 1) {
      String[] triples = metadata[0].split("\t");
      for (String triple : triples) {
        doc.add("triple", triple);
      }
    }
    return doc;
  }
}
