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
  public Parse filter(String url, WebPage page, Parse parse,
      HTMLMetaTags metaTags, DocumentFragment doc) {

    // loop on each filter
    for (HtmlParseFilter htmlParseFilter : htmlParseFilters) {
      // call filter interface
      parse = htmlParseFilter.filter(url, page, parse, metaTags, doc);

      // any failure on parse obj, return
      if (!ParseStatusUtils.isSuccess(parse.getParseStatus())) {
        return parse;
      }
    }

    return parse;
  }

  public Collection<WebPage.Field> getFields() {
    Collection<WebPage.Field> fields = new HashSet<WebPage.Field>();
    for (HtmlParseFilter htmlParseFilter : htmlParseFilters) {
      Collection<WebPage.Field> pluginFields = htmlParseFilter.getFields();
      if (pluginFields != null) {
        fields.addAll(pluginFields);
      }
    }
    return fields;
  }
}
