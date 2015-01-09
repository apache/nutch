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

package org.apache.nutch.indexer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.ObjectCache;

/** Creates and caches {@link IndexCleaningFilter} implementing plugins. */
public class IndexCleaningFilters {

  public static final String IndexCleaningFilter_ORDER = "IndexCleaningFilterhbase.order";

  public final static Logger LOG = LoggerFactory
      .getLogger(IndexCleaningFilters.class);

  private IndexCleaningFilter[] indexcleaningFilters;

  public IndexCleaningFilters(Configuration conf) {
    /* Get IndexCleaningFilter.order property */
    String order = conf.get(IndexCleaningFilter_ORDER);
    ObjectCache objectCache = ObjectCache.get(conf);
    this.indexcleaningFilters = (IndexCleaningFilter[]) objectCache
        .getObject(IndexCleaningFilter.class.getName());
    if (this.indexcleaningFilters == null) {
      /*
       * If ordered filters are required, prepare array of filters based on
       * property
       */
      String[] orderedFilters = null;
      if (order != null && !order.trim().equals("")) {
        orderedFilters = order.split("\\s+");
      }
      try {
        ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(
            IndexCleaningFilter.X_POINT_ID);
        if (point == null)
          throw new RuntimeException(IndexCleaningFilter.X_POINT_ID
              + " not found.");
        Extension[] extensions = point.getExtensions();
        HashMap<String, IndexCleaningFilter> filterMap = new HashMap<String, IndexCleaningFilter>();
        for (int i = 0; i < extensions.length; i++) {
          Extension extension = extensions[i];
          IndexCleaningFilter filter = (IndexCleaningFilter) extension
              .getExtensionInstance();
          LOG.info("Adding " + filter.getClass().getName());
          if (!filterMap.containsKey(filter.getClass().getName())) {
            filterMap.put(filter.getClass().getName(), filter);
          }
        }
        /*
         * If no ordered filters required, just get the filters in an
         * indeterminate order
         */
        if (orderedFilters == null) {
          objectCache.setObject(IndexCleaningFilter.class.getName(), filterMap
              .values().toArray(new IndexCleaningFilter[0]));
          /* Otherwise run the filters in the required order */
        } else {
          ArrayList<IndexCleaningFilter> filters = new ArrayList<IndexCleaningFilter>();
          for (int i = 0; i < orderedFilters.length; i++) {
            IndexCleaningFilter filter = filterMap.get(orderedFilters[i]);
            if (filter != null) {
              filters.add(filter);
            }
          }
          objectCache.setObject(IndexCleaningFilter.class.getName(),
              filters.toArray(new IndexCleaningFilter[filters.size()]));
        }
      } catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }
      this.indexcleaningFilters = (IndexCleaningFilter[]) objectCache
          .getObject(IndexCleaningFilter.class.getName());
    }
  }

  /** Run all defined filters. */
  public boolean remove(String url, WebPage page) throws IndexingException {
    for (IndexCleaningFilter indexcleaningFilter : indexcleaningFilters) {
      if (indexcleaningFilter.remove(url, page)) {
        return true;
      }
    }
    return false;
  }

  public Collection<WebPage.Field> getFields() {
    Collection<WebPage.Field> columns = new HashSet<WebPage.Field>();
    for (IndexCleaningFilter indexcleaningFilter : indexcleaningFilters) {
      Collection<WebPage.Field> fields = indexcleaningFilter.getFields();
      if (fields != null) {
        columns.addAll(fields);
      }
    }
    return columns;
  }

}
