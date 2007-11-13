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
package org.apache.nutch.indexer.anchor;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.parse.Parse;

/**
 * Indexing filter that indexes all inbound anchor text for a document. 
 */
public class AnchorIndexingFilter
  implements IndexingFilter {

  public static final Log LOG = LogFactory.getLog(AnchorIndexingFilter.class);
  private Configuration conf;

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

  public Document filter(Document doc, Parse parse, Text url, CrawlDatum datum,
    Inlinks inlinks) throws IndexingException {

    try {
      String[] anchors = (inlinks != null ? inlinks.getAnchors()
        : new String[0]);
      for (int i = 0; i < anchors.length; i++) {
        doc.add(new Field("anchor", anchors[i], Field.Store.NO,
          Field.Index.TOKENIZED));
      }
    } catch (IOException ioe) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("AnchorIndexingFilter: can't get anchors for "
          + url.toString());
      }
    }

    return doc;
  }
}
