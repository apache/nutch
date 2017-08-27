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
package org.apache.nutch.analysis.lang;

// Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.hadoop.io.Text;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;

import java.util.HashSet;
import java.util.Set;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;

/**
 * An {@link org.apache.nutch.indexer.IndexingFilter} that add a
 * <code>lang</code> (language) field to the document.
 * 
 * It tries to find the language of the document by:
 * <ul>
 * <li>First, checking if {@link HTMLLanguageParser} add some language
 * information</li>
 * <li>Then, checking if a <code>Content-Language</code> HTTP header can be
 * found</li>
 * <li>Finaly by analyzing the document content</li>
 * </ul>
 * 
 * @author Sami Siren
 * @author Jerome Charron
 */
public class LanguageIndexingFilter implements IndexingFilter {

  private Configuration conf;
  private Set<String> indexLangs;

  /**
   * Constructs a new Language Indexing Filter.
   */
  public LanguageIndexingFilter() {

  }

  // Inherited JavaDoc
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {

    // check if LANGUAGE found, possibly put there by HTMLLanguageParser
    String lang = parse.getData().getParseMeta().get(Metadata.LANGUAGE);

    // check if HTTP-header tels us the language
    if (lang == null) {
      lang = parse.getData().getContentMeta().get(Response.CONTENT_LANGUAGE);
    }

    if (lang == null || lang.length() == 0) {
      lang = "unknown";
    }

    if (!indexLangs.isEmpty() && !indexLangs.contains(lang)) {
    	return null;
    }
    
    doc.add("lang", lang);

    return doc;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    indexLangs = new HashSet<>(conf.getStringCollection("lang.index.languages"));
  }

  public Configuration getConf() {
    return this.conf;
  }

}
