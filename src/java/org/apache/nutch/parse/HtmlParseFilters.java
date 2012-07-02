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

package org.apache.nutch.parse;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.plugin.*;
import org.apache.nutch.util.ObjectCache;
import org.apache.hadoop.conf.Configuration;

import org.w3c.dom.DocumentFragment;

/** Creates and caches {@link HtmlParseFilter} implementing plugins.*/
public class HtmlParseFilters {

  private HtmlParseFilter[] htmlParseFilters;
  
  public static final String HTMLPARSEFILTER_ORDER = "htmlparsefilter.order";

  public HtmlParseFilters(Configuration conf) {
        String order = conf.get(HTMLPARSEFILTER_ORDER);
        ObjectCache objectCache = ObjectCache.get(conf);
        this.htmlParseFilters = (HtmlParseFilter[]) objectCache.getObject(HtmlParseFilter.class.getName());
        if (htmlParseFilters == null) {
          /*
           * If ordered filters are required, prepare array of filters based on
           * property
           */
          String[] orderedFilters = null;
          if (order != null && !order.trim().equals("")) {
            orderedFilters = order.split("\\s+");
          }
            HashMap<String, HtmlParseFilter> filterMap =
              new HashMap<String, HtmlParseFilter>();
            try {
                ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(HtmlParseFilter.X_POINT_ID);
                if (point == null)
                    throw new RuntimeException(HtmlParseFilter.X_POINT_ID + " not found.");
                Extension[] extensions = point.getExtensions();
                for (int i = 0; i < extensions.length; i++) {
                    Extension extension = extensions[i];
                    HtmlParseFilter parseFilter = (HtmlParseFilter) extension.getExtensionInstance();
                    if (!filterMap.containsKey(parseFilter.getClass().getName())) {
                        filterMap.put(parseFilter.getClass().getName(), parseFilter);
                    }
                }
                HtmlParseFilter[] htmlParseFilters = filterMap.values().toArray(new HtmlParseFilter[filterMap.size()]);
                /*
                 * If no ordered filters required, just get the filters in an
                 * indeterminate order
                 */
                if (orderedFilters == null) {
                  objectCache.setObject(HtmlParseFilter.class.getName(), htmlParseFilters);
                }
                /* Otherwise run the filters in the required order */
                else {
                  ArrayList<HtmlParseFilter> filters = new ArrayList<HtmlParseFilter>();
                  for (int i = 0; i < orderedFilters.length; i++) {
                    HtmlParseFilter filter = filterMap
                        .get(orderedFilters[i]);
                    if (filter != null) {
                      filters.add(filter);
                    }
                  }
                  objectCache.setObject(HtmlParseFilter.class.getName(), filters
                      .toArray(new HtmlParseFilter[filters.size()]));
                }
            } catch (PluginRuntimeException e) {
                throw new RuntimeException(e);
            }
            this.htmlParseFilters = (HtmlParseFilter[]) objectCache.getObject(HtmlParseFilter.class.getName());
        }
    }                  

  /** Run all defined filters. */
  public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {

    // loop on each filter
    for (int i = 0 ; i < this.htmlParseFilters.length; i++) {
      // call filter interface
      parseResult =
        htmlParseFilters[i].filter(content, parseResult, metaTags, doc);

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
