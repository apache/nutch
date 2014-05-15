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

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;

/**
 * Indexer which can be configured to extract metadata from the crawldb, parse
 * metadata or content metadata. You can specify the properties "index.db",
 * "index.parse" or "index.content" who's values are comma-delimited
 * <value>key1, key2, key3</value>.
 */

public class MetadataIndexer implements IndexingFilter {
  private Configuration conf;
  private static String[] parseFieldnames;
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
      for (String metatag : parseFieldnames) {
        ByteBuffer bvalues = page.getMetadata().get(new Utf8(PARSE_META_PREFIX
            + metatag));
        if (bvalues != null) {
          String value = new String(bvalues.array());
          String[] values = value.split("\t");
          for (String eachvalue : values) {
            doc.add(INDEX_PREFIX + metatag, eachvalue);
          }
        }
      }
    }

    return doc;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    parseFieldnames = conf.getStrings(PARSE_CONF_PROPERTY);
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
