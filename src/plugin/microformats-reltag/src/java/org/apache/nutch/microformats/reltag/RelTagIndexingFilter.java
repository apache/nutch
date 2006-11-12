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
package org.apache.nutch.microformats.reltag;


// Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;
import org.apache.hadoop.io.Text;
import org.apache.nutch.parse.Parse;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;

// Lucene imports
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Document;


/**
 * An {@link org.apache.nutch.indexer.IndexingFilter} that 
 * add <code>tag</code> field(s) to the document.
 *
 * @see <a href="http://www.microformats.org/wiki/rel-tag">
 *      http://www.microformats.org/wiki/rel-tag</a>
 * @author J&eacute;r&ocirc;me Charron
 */
public class RelTagIndexingFilter implements IndexingFilter {
  

  private Configuration conf;


  // Inherited JavaDoc
  public Document filter(Document doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks)
    throws IndexingException {

    // Check if some Rel-Tags found, possibly put there by RelTagParser
    String[] tags = parse.getData().getParseMeta().getValues(RelTagParser.REL_TAG);
    if (tags != null) {
      for (int i=0; i<tags.length; i++) {
        doc.add(new Field("tag", tags[i],
                          Field.Store.YES, Field.Index.UN_TOKENIZED));
      }
    }

    return doc;
  }

  
  /* ----------------------------- *
   * <implementation:Configurable> *
   * ----------------------------- */
  
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
  
  /* ------------------------------ *
   * </implementation:Configurable> *
   * ------------------------------ */
  
}
