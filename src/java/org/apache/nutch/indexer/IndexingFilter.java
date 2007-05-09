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

package org.apache.nutch.indexer;

// Lucene imports
import org.apache.lucene.document.Document;

// Hadoop imports
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.Text;

// Nutch imports
import org.apache.nutch.parse.Parse;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.plugin.Pluggable;


/** Extension point for indexing.  Permits one to add metadata to the indexed
 * fields.  All plugins found which implement this extension point are run
 * sequentially on the parse.
 */
public interface IndexingFilter extends Pluggable, Configurable {
  /** The name of the extension point. */
  final static String X_POINT_ID = IndexingFilter.class.getName();

  /**
   * Adds fields or otherwise modifies the document that will be indexed for a
   * parse. Unwanted documents can be removed from indexing by returning a null value.
   * 
   * @param doc document instance for collecting fields
   * @param parse parse data instance
   * @param url page url
   * @param datum crawl datum for the page
   * @param inlinks page inlinks
   * @return modified (or a new) document instance, or null (meaning the document
   * should be discarded)
   * @throws IndexingException
   */
  Document filter(Document doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks)
    throws IndexingException;
}
