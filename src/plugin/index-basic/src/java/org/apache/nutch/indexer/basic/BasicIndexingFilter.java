/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.indexer.basic;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import org.apache.nutch.parse.Parse;

import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;
import org.apache.hadoop.io.UTF8;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Logger;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.conf.Configuration;

/** Adds basic searchable fields to a document. */
public class BasicIndexingFilter implements IndexingFilter {
  public static final Logger LOG
    = LogFormatter.getLogger(BasicIndexingFilter.class.getName());

  private int MAX_TITLE_LENGTH;
  private Configuration conf;

  public Document filter(Document doc, Parse parse, UTF8 url, CrawlDatum datum, Inlinks inlinks)
    throws IndexingException {
    
    String host = null;
    try {
      URL u = new URL(url.toString());
      host = u.getHost();
    } catch (MalformedURLException e) {
      throw new IndexingException(e);
    }

    if (host != null) {
      // add host as un-stored, indexed and tokenized
      doc.add(new Field("host", host, Field.Store.NO, Field.Index.TOKENIZED));
      // add site as un-stored, indexed and un-tokenized
      doc.add(new Field("site", host, Field.Store.NO, Field.Index.UN_TOKENIZED));
    }


    // url is both stored and indexed, so it's both searchable and returned
    doc.add(new Field("url", url.toString(), Field.Store.YES, Field.Index.TOKENIZED));
    
    // content is indexed, so that it's searchable, but not stored in index
    doc.add(new Field("content", parse.getText(), Field.Store.NO, Field.Index.TOKENIZED));
    
    // anchors are indexed, so they're searchable, but not stored in index
    try {
      String[] anchors = (inlinks != null ? inlinks.getAnchors() : new String[0]);
      for (int i = 0; i < anchors.length; i++) {
        doc.add(new Field("anchor", anchors[i], Field.Store.NO, Field.Index.TOKENIZED));
      }
    } catch (IOException ioe) {
      LOG.warning("BasicIndexingFilter: can't get anchors for " + url.toString());
    }

    // title
    String title = parse.getData().getTitle();
    if (title.length() > MAX_TITLE_LENGTH) {      // truncate title if needed
      title = title.substring(0, MAX_TITLE_LENGTH);
    }
    // add title indexed and stored so that it can be displayed
    doc.add(new Field("title", title, Field.Store.YES, Field.Index.TOKENIZED));

    return doc;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.MAX_TITLE_LENGTH = conf.getInt("indexer.max.title.length", 100);
  }

  public Configuration getConf() {
    return this.conf;
  }

}
