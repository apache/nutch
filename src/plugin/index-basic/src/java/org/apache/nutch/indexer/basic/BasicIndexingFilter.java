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

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.TableUtil;
import org.apache.solr.common.util.DateUtil;

/** Adds basic searchable fields to a document. */
public class BasicIndexingFilter implements IndexingFilter {
  public static final Logger LOG = LoggerFactory.getLogger(BasicIndexingFilter.class);

  private int MAX_TITLE_LENGTH;
  private Configuration conf;

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.TITLE);
    FIELDS.add(WebPage.Field.TEXT);
    FIELDS.add(WebPage.Field.FETCH_TIME);
  }

  public NutchDocument filter(NutchDocument doc, String url, WebPage page)
      throws IndexingException {

    String reprUrl = null;
    if (page.isReadable(WebPage.Field.REPR_URL.getIndex())) {
      reprUrl = TableUtil.toString(page.getReprUrl());
    }

    String host = null;
    try {
      URL u;
      if (reprUrl != null) {
        u = new URL(reprUrl);
      } else {
        u = new URL(url);
      }
      host = u.getHost();
    } catch (MalformedURLException e) {
      throw new IndexingException(e);
    }

    if (host != null) {
      // add host as un-stored, indexed and tokenized
      doc.add("host", host);
      // add site as un-stored, indexed and un-tokenized
      doc.add("site", host);
    }

    // url is both stored and indexed, so it's both searchable and returned
    doc.add("url", reprUrl == null ? url : reprUrl);

    if (reprUrl != null) {
      // also store original url as both stored and indexes
      doc.add("orig", url);
    }

    // content is indexed, so that it's searchable, but not stored in index
    doc.add("content", TableUtil.toString(page.getText()));

    // title
    String title = TableUtil.toString(page.getTitle());
    if (title.length() > MAX_TITLE_LENGTH) { // truncate title if needed
      title = title.substring(0, MAX_TITLE_LENGTH);
    }
    if (title.length() > 0) {
      // NUTCH-1004 Do not index empty values for title field
      doc.add("title", title);
    }
    // add cached content/summary display policy, if available
    ByteBuffer cachingRaw = page
        .getFromMetadata(Nutch.CACHING_FORBIDDEN_KEY_UTF8);
    String caching = (cachingRaw == null ? null : Bytes.toString(cachingRaw
        .array()));
    if (caching != null && !caching.equals(Nutch.CACHING_FORBIDDEN_NONE)) {
      doc.add("cache", caching);
    }

    // add timestamp when fetched, for deduplication
    String tstamp = DateUtil.getThreadLocalDateFormat().format(new Date(page.getFetchTime()));
    doc.add("tstamp", tstamp);

    return doc;
  }

  public void addIndexBackendOptions(Configuration conf) {
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    this.MAX_TITLE_LENGTH = conf.getInt("indexer.max.title.length", 100);
  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public Collection<WebPage.Field> getFields() {
    return FIELDS;
  }

}
