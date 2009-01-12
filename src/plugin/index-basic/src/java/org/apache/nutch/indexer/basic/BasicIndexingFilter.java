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

package org.apache.nutch.indexer.basic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.lucene.document.DateTools;

import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;

import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.lucene.LuceneWriter;
import org.apache.hadoop.io.Text;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;

import java.net.MalformedURLException;
import java.net.URL;
import org.apache.hadoop.conf.Configuration;

/** Adds basic searchable fields to a document. */
public class BasicIndexingFilter implements IndexingFilter {
  public static final Log LOG = LogFactory.getLog(BasicIndexingFilter.class);

  private int MAX_TITLE_LENGTH;
  private Configuration conf;

  public NutchDocument filter(NutchDocument doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks)
    throws IndexingException {

    Text reprUrl = (Text) datum.getMetaData().get(Nutch.WRITABLE_REPR_URL_KEY);
    String reprUrlString = reprUrl != null ? reprUrl.toString() : null;
    String urlString = url.toString();
    
    String host = null;
    try {
      URL u;
      if (reprUrlString != null) {
        u = new URL(reprUrlString);
      } else {
        u = new URL(urlString);
      }
      host = u.getHost();
    } catch (MalformedURLException e) {
      throw new IndexingException(e);
    }

    if (host != null) {
      doc.add("host", host);
      doc.add("site", host);
    }

    doc.add("url", reprUrlString == null ? urlString : reprUrlString);
    doc.add("content", parse.getText());
    
    // title
    String title = parse.getData().getTitle();
    if (title.length() > MAX_TITLE_LENGTH) {      // truncate title if needed
      title = title.substring(0, MAX_TITLE_LENGTH);
    }
    doc.add("title", title);

    // add cached content/summary display policy, if available
    String caching = parse.getData().getMeta(Nutch.CACHING_FORBIDDEN_KEY);
    if (caching != null && !caching.equals(Nutch.CACHING_FORBIDDEN_NONE)) {
      doc.add("cache", caching);
    }
    
    // add timestamp when fetched, for deduplication
    doc.add("tstamp",
            DateTools.timeToString(datum.getFetchTime(),
            DateTools.Resolution.MILLISECOND));

    return doc;
  }

  public void addIndexBackendOptions(Configuration conf) {

    ///////////////////////////
    //    add lucene options   //
    ///////////////////////////

    // host is un-stored, indexed and tokenized
    LuceneWriter.addFieldOptions("host", LuceneWriter.STORE.NO,
        LuceneWriter.INDEX.TOKENIZED, conf);

    // site is un-stored, indexed and un-tokenized
    LuceneWriter.addFieldOptions("site", LuceneWriter.STORE.NO,
        LuceneWriter.INDEX.UNTOKENIZED, conf);

    // url is both stored and indexed, so it's both searchable and returned
    LuceneWriter.addFieldOptions("url", LuceneWriter.STORE.YES,
        LuceneWriter.INDEX.TOKENIZED, conf);

    // content is indexed, so that it's searchable, but not stored in index
    LuceneWriter.addFieldOptions("content", LuceneWriter.STORE.NO,
        LuceneWriter.INDEX.TOKENIZED, conf);

    // anchors are indexed, so they're searchable, but not stored in index
    LuceneWriter.addFieldOptions("anchor", LuceneWriter.STORE.NO,
        LuceneWriter.INDEX.TOKENIZED, conf);

    // title is indexed and stored so that it can be displayed
    LuceneWriter.addFieldOptions("title", LuceneWriter.STORE.YES,
        LuceneWriter.INDEX.TOKENIZED, conf);

    LuceneWriter.addFieldOptions("cache", LuceneWriter.STORE.YES, LuceneWriter.INDEX.NO, conf);
    LuceneWriter.addFieldOptions("tstamp", LuceneWriter.STORE.YES, LuceneWriter.INDEX.NO, conf);
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.MAX_TITLE_LENGTH = conf.getInt("indexer.max.title.length", 100);
  }

  public Configuration getConf() {
    return this.conf;
  }

}
