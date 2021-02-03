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
package org.apache.nutch.parse;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.hadoop.conf.Configuration;

import org.w3c.dom.DocumentFragment;

/** Creates and caches {@link HtmlParseFilter} implementing plugins. */
public class HtmlParseFilters {

  private HtmlParseFilter[] htmlParseFilters;

  public static final String HTMLPARSEFILTER_ORDER = "htmlparsefilter.order";

  public HtmlParseFilters(Configuration conf) {
    htmlParseFilters = (HtmlParseFilter[]) PluginRepository.get(conf)
        .getOrderedPlugins(HtmlParseFilter.class, HtmlParseFilter.X_POINT_ID,
            HTMLPARSEFILTER_ORDER);
  }

  /**
   * Run all defined filters.
   * @param content the {@link Content} for a given response
   * @param parseResult the result of running on or more 
   * {@link Parser}'s on the content. 
   * @see Parser#getParse(Content)
   * @param metaTags a populated {@link HTMLMetaTags} object
   * @param doc a {@link DocumentFragment} (DOM) which can be processed in
   * the filtering process.
   * @return a filtered {@link ParseResult}
   */
  public ParseResult filter(Content content, ParseResult parseResult,
      HTMLMetaTags metaTags, DocumentFragment doc) {

    // loop on each filter
    for (int i = 0; i < this.htmlParseFilters.length; i++) {
      // call filter interface
      parseResult = htmlParseFilters[i].filter(content, parseResult, metaTags,
          doc);

      // any failure on parse obj, return
      if (!parseResult.isSuccess()) {
        // TODO: What happens when parseResult.isEmpty() ?
        // Maybe clone parseResult and use parseResult as backup...

        // remove failed parse before return
        parseResult.filter();
        return parseResult;
      }
    }

    return parseResult;
  }
}
