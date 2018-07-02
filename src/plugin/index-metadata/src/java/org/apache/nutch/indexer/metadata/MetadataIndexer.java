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

package org.apache.nutch.indexer.metadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;

/**
 * Indexer which can be configured to extract metadata from the crawldb, parse
 * metadata or content metadata. You can specify the properties "index.db.md",
 * "index.parse.md" or "index.content.md" who's values are comma-delimited
 * <value>key1,key2,key3</value>.
 */
public class MetadataIndexer implements IndexingFilter {
  private Configuration conf;
  private String[] dbFieldnames;
  private Map<String, String> parseFieldnames;
  private String[] contentFieldnames;
  private String separator;
  private Set<String> mvFields;
  private static final String db_CONF_PROPERTY = "index.db.md";
  private static final String parse_CONF_PROPERTY = "index.parse.md";
  private static final String content_CONF_PROPERTY = "index.content.md";
  private static final String separator_CONF_PROPERTY = "index.metadata.separator";
  private static final String mvfields_CONF_PROPERTY = "index.metadata.multivalued.fields";
  
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {

    // just in case
    if (doc == null)
      return doc;

    // add the fields from crawldb
    if (dbFieldnames != null) {
      for (String metatag : dbFieldnames) {
        Writable metadata = datum.getMetaData().get(new Text(metatag));
        if (metadata != null)
          add(doc, metatag, metadata.toString());
      }
    }

    // add the fields from parsemd
    if (parseFieldnames != null) {
      for (String metatag : parseFieldnames.keySet()) {
        for (String value : parse.getData().getParseMeta().getValues(metatag)) {
          if (value != null)
            add(doc, parseFieldnames.get(metatag), value);
        }
      }
    }

    // add the fields from contentmd
    if (contentFieldnames != null) {
      for (String metatag : contentFieldnames) {
        for (String value : parse.getData().getContentMeta().getValues(metatag)) {
          if (value != null)
            add(doc, metatag, value);
        }
      }
    }

    return doc;
  }
  
  protected void add(NutchDocument doc, String key, String value) {
    if (separator == null || value.indexOf(separator) == -1 || !mvFields.contains(key)) {
      doc.add(key, value);
    } else {
      String[] parts = value.split(separator);
      for (String part : parts) {
        part = part.trim();
        if (part.length() != 0) {
          doc.add(key, part);
        }
      }
    }
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    dbFieldnames = conf.getStrings(db_CONF_PROPERTY);
    parseFieldnames = new HashMap<String, String>();
    for (String metatag : conf.getStrings(parse_CONF_PROPERTY)) {
      parseFieldnames.put(metatag.toLowerCase(Locale.ROOT), metatag);
    }
    contentFieldnames = conf.getStrings(content_CONF_PROPERTY);
    
    separator = conf.get(separator_CONF_PROPERTY, null);
    mvFields = new HashSet(Arrays.asList(conf.getStrings(mvfields_CONF_PROPERTY, new String[0])));
    // TODO check conflict between field names e.g. could have same label
    // from different sources

  }

  public Configuration getConf() {
    return this.conf;
  }
}
