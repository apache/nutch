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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;

import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
import org.apache.hadoop.io.Text;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;

/**
 * Adds basic searchable fields to a document. The fields added are : domain,
 * host, url, content, title, cache, tstamp domain is included depending on
 * {@code indexer.add.domain} in nutch-default.xml. title is truncated as per
 * {@code indexer.max.title.length} in nutch-default.xml. (As per NUTCH-1004, a
 * zero-length title is not added) content is truncated as per
 * {@code indexer.max.content.length} in nutch-default.xml.
 */
public class BasicIndexingFilter implements IndexingFilter {
  public static final Logger LOG = LoggerFactory
      .getLogger(BasicIndexingFilter.class);

  private int MAX_TITLE_LENGTH;
  private int MAX_CONTENT_LENGTH;
  private boolean addDomain = false;
  private Configuration conf;

  /**
   * The {@link BasicIndexingFilter} filter object which supports few
   * configuration settings for adding basic searchable fields. See
   * {@code indexer.add.domain}, {@code indexer.max.title.length},
   * {@code indexer.max.content.length} in nutch-default.xml.
   * 
   * @param doc
   *          The {@link NutchDocument} object
   * @param parse
   *          The relevant {@link Parse} object passing through the filter
   * @param url
   *          URL to be filtered for anchor text
   * @param datum
   *          The {@link CrawlDatum} entry
   * @param inlinks
   *          The {@link Inlinks} containing anchor text
   * @return filtered NutchDocument
   */
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {

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

      if (addDomain) {
        doc.add("domain", URLUtil.getDomainName(u));
      }

      host = u.getHost();
    } catch (MalformedURLException e) {
      throw new IndexingException(e);
    }

    if (host != null) {
      doc.add("host", host);
    }

    doc.add("url", reprUrlString == null ? urlString : reprUrlString);

    // content
    String content = parse.getText();
    if (MAX_CONTENT_LENGTH > -1 && content.length() > MAX_CONTENT_LENGTH) {
      content = content.substring(0, MAX_CONTENT_LENGTH);
    }
    doc.add("content", StringUtil.cleanField(content));

    // title
    String title = parse.getData().getTitle();
    if (MAX_TITLE_LENGTH > -1 && title.length() > MAX_TITLE_LENGTH) { // truncate
                                                                      // title
                                                                      // if
                                                                      // needed
      title = title.substring(0, MAX_TITLE_LENGTH);
    }

    if (title.length() > 0) {
      // NUTCH-1004 Do not index empty values for title field
      doc.add("title", StringUtil.cleanField(title));
    }

    // add cached content/summary display policy, if available
    String caching = parse.getData().getMeta(Nutch.CACHING_FORBIDDEN_KEY);
    if (caching != null && !caching.equals(Nutch.CACHING_FORBIDDEN_NONE)) {
      doc.add("cache", caching);
    }

    // add timestamp when fetched, for deduplication
    doc.add("tstamp", new Date(datum.getFetchTime()));

    return doc;
  }

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.MAX_TITLE_LENGTH = conf.getInt("indexer.max.title.length", 100);
    this.addDomain = conf.getBoolean("indexer.add.domain", false);
    this.MAX_CONTENT_LENGTH = conf.getInt("indexer.max.content.length", -1);
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return this.conf;
  }

}
