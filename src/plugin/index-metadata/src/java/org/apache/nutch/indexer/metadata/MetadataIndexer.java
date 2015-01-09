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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.Bytes;

/**
 * Indexer which can be configured to extract metadata from the crawldb, parse
 * metadata or content metadata. You can specify the properties "index.db",
 * "index.parse" or "index.content" who's values are comma-delimited
 * <value>key1,key2,key3</value>.
 */

public class MetadataIndexer implements IndexingFilter {
  private Configuration conf;
  private static Map<Utf8, String> parseFieldnames;
  private static final String PARSE_CONF_PROPERTY = "index.metadata";
  private static final String INDEX_PREFIX = "meta_";
  private static final String PARSE_META_PREFIX = "meta_";

  public NutchDocument filter(NutchDocument doc, String url, WebPage page)
      throws IndexingException {

    // just in case
    if (doc == null)
      return doc;

    // add the fields from parsemd
    if (parseFieldnames != null) {
      for (Entry<Utf8, String> metatag : parseFieldnames.entrySet()) {
        ByteBuffer bvalues = page.getMetadata().get(metatag.getKey());
        if (bvalues != null) {
          String key = metatag.getValue();
          String value = Bytes.toString(bvalues.array());
          String[] values = value.split("\t");
          for (String eachvalue : values) {
            doc.add(key, eachvalue);
          }
        }
      }
    }

    return doc;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    String[] metatags = conf.getStrings(PARSE_CONF_PROPERTY);
    parseFieldnames = new TreeMap<Utf8, String>();
    for (int i = 0; i < metatags.length; i++) {
      parseFieldnames.put(
          new Utf8(PARSE_META_PREFIX + metatags[i].toLowerCase(Locale.ROOT)),
          INDEX_PREFIX + metatags[i]);
    }
    // TODO check conflict between field names e.g. could have same label
    // from different sources
  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public Collection<Field> getFields() {
    return null;
  }
}
