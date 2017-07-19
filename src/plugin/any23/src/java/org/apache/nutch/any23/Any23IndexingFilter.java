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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.Bytes;

/**
 * <p>This implementation of {@link org.apache.nutch.indexer.IndexingFilter}
 * adds a <i>triple(s)</i> field to the {@link org.apache.nutch.indexer.NutchDocument}.</p>
 * <p>Triples are extracted via <a href="http://any23.apache.org">Apache Any23</a>.</p>
 * @see {@link org.apache.nutch.any23.Any23ParseFilter}.
 */
public class Any23IndexingFilter implements IndexingFilter {
  
  private Configuration conf;
  
  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.BASE_URL);
    FIELDS.add(WebPage.Field.METADATA);
  }

  /** 
   * Gets all the fields for a given {@link WebPage}
   * Many datastores need to setup the mapreduce job by specifying the fields
   * needed. All extensions that work on {@link org.apache.nutch.storage.WebPage} 
   * are able to specify what fields they need.
   *
   * @see org.apache.nutch.plugin.FieldPluggable#getFields()
   */
  @Override
  public Collection<Field> getFields() {
    return FIELDS;
  }

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
   * @param doc The {@link NutchDocument} object
   * @param url URL to be filtered for triples.
   * @param page {@link WebPage} object relative to the URL
   * @return filtered NutchDocument
   * @see org.apache.nutch.indexer.IndexingFilter#filter(org.apache.nutch.indexer.NutchDocument, java.lang.String, org.apache.nutch.storage.WebPage)
   */
  @Override
  public NutchDocument filter(NutchDocument doc, String url, WebPage page) throws IndexingException {
    // Check if some Triples are found, possibly put there by Any23ParseFilter
    ByteBuffer bb = page.getFromMetadata(new Utf8(Any23ParseFilter.ANY23_TRIPLES));

    if (bb != null) {
      String[] triples = Bytes.toString(bb).split("\t");
      for (int i = 0; i < triples.length; i++) {
        doc.add("triple", triples[i]);
      }
    }
    return doc;
  }

}
