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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.ObjectCache;
import org.w3c.dom.DocumentFragment;

/** Creates and caches {@link ParseFilter} implementing plugins.*/
public class ParseFilters {

  private ParseFilter[] parseFilters;

  public static final String HTMLPARSEFILTER_ORDER = "htmlparsefilter.order";

  public ParseFilters(Configuration conf) {
    String order = conf.get(HTMLPARSEFILTER_ORDER);
    ObjectCache objectCache = ObjectCache.get(conf);
    this.parseFilters = (ParseFilter[]) objectCache.getObject(ParseFilter.class.getName());
    if (parseFilters == null) {
      /*
       * If ordered filters are required, prepare array of filters based on
       * property
       */
      String[] orderedFilters = null;
      if (order != null && !order.trim().equals("")) {
        orderedFilters = order.split("\\s+");
      }
      HashMap<String, ParseFilter> filterMap =
        new HashMap<String, ParseFilter>();
      try {
        ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(ParseFilter.X_POINT_ID);
        if (point == null)
          throw new RuntimeException(ParseFilter.X_POINT_ID + " not found.");
        Extension[] extensions = point.getExtensions();
        for (int i = 0; i < extensions.length; i++) {
          Extension extension = extensions[i];
          ParseFilter parseFilter = (ParseFilter) extension.getExtensionInstance();
          if (!filterMap.containsKey(parseFilter.getClass().getName())) {
            filterMap.put(parseFilter.getClass().getName(), parseFilter);
          }
        }
        ParseFilter[] htmlParseFilters = filterMap.values().toArray(new ParseFilter[filterMap.size()]);
        /*
         * If no ordered filters required, just get the filters in an
         * indeterminate order
         */
        if (orderedFilters == null) {
          objectCache.setObject(ParseFilter.class.getName(), htmlParseFilters);
        }
        /* Otherwise run the filters in the required order */
        else {
          ArrayList<ParseFilter> filters = new ArrayList<ParseFilter>();
          for (int i = 0; i < orderedFilters.length; i++) {
            ParseFilter filter = filterMap
            .get(orderedFilters[i]);
            if (filter != null) {
              filters.add(filter);
            }
          }
          objectCache.setObject(ParseFilter.class.getName(), filters
              .toArray(new ParseFilter[filters.size()]));
        }
      } catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }
      this.parseFilters = (ParseFilter[]) objectCache.getObject(ParseFilter.class.getName());
    }
  }

  /** Run all defined filters. */
  public Parse filter(String url, WebPage page, Parse parse,
      HTMLMetaTags metaTags, DocumentFragment doc) {

    // loop on each filter
    for (ParseFilter parseFilter : parseFilters) {
      // call filter interface
      parse = parseFilter.filter(url, page, parse, metaTags, doc);

      // any failure on parse obj, return
      if (!ParseStatusUtils.isSuccess(parse.getParseStatus())) {
        return parse;
      }
    }

    return parse;
  }

  public Collection<WebPage.Field> getFields() {
    Collection<WebPage.Field> fields = new HashSet<WebPage.Field>();
    for (ParseFilter htmlParseFilter : parseFilters) {
      Collection<WebPage.Field> pluginFields = htmlParseFilter.getFields();
      if (pluginFields != null) {
        fields.addAll(pluginFields);
      }
    }
    return fields;
  }
}
