/*
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
package org.apache.nutch.parse.metatags;

import java.lang.invoke.MethodHandles;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

/**
 * Parse HTML meta tags (keywords, description) and store them in the parse
 * metadata so that they can be indexed with the index-metadata plugin with the
 * prefix 'metatag.'. Metatags are matched ignoring case.
 */
public class MetaTagsParser implements HtmlParseFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration conf;

  private Set<String> metatagset = new HashSet<String>();

  public void setConf(Configuration conf) {
    this.conf = conf;
    // specify whether we want a specific subset of metadata
    // by default take everything we can find
    String[] values = conf.getStrings("metatags.names", "*");
    for (String val : values) {
      metatagset.add(val.toLowerCase(Locale.ROOT));
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Check whether the metatag is in the list of metatags to be indexed (or if
   * '*' is specified). If yes, add it to parse metadata.
   */
  private void addIndexedMetatags(Metadata metadata, String metatag,
      String value) {
    String lcMetatag = metatag.toLowerCase(Locale.ROOT);
    if (metatagset.contains("*") || metatagset.contains(lcMetatag)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found meta tag: {}\t{}", lcMetatag, value);
      }
      metadata.add("metatag." + lcMetatag, value);
    }
  }

  /**
   * Check whether the metatag is in the list of metatags to be indexed (or if
   * '*' is specified). If yes, add it with all values to parse metadata.
   */
  private void addIndexedMetatags(Metadata metadata, String metatag,
      String[] values) {
    String lcMetatag = metatag.toLowerCase(Locale.ROOT);
    if (metatagset.contains("*") || metatagset.contains(lcMetatag)) {
      String key = "metatag." + lcMetatag;
      for (String value : values) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found meta tag: {}\t{}", lcMetatag, value);
        }
        metadata.add(key, value);
      }
    }
  }

  public ParseResult filter(Content content, ParseResult parseResult,
      HTMLMetaTags metaTags, DocumentFragment doc) {

    Parse parse = parseResult.get(content.getUrl());
    Metadata metadata = parse.getData().getParseMeta();

    /*
     * NUTCH-1559: do not extract meta values from ParseData's metadata to avoid
     * duplicate metatag values
     */

    Metadata generalMetaTags = metaTags.getGeneralTags();
    for (String tagName : generalMetaTags.names()) {
      addIndexedMetatags(metadata, tagName, generalMetaTags.getValues(tagName));
    }

    Properties httpequiv = metaTags.getHttpEquivTags();
    for (Enumeration<?> tagNames = httpequiv.propertyNames(); tagNames
        .hasMoreElements();) {
      String name = (String) tagNames.nextElement();
      String value = httpequiv.getProperty(name);
      addIndexedMetatags(metadata, name, value);
    }

    return parseResult;
  }

}
