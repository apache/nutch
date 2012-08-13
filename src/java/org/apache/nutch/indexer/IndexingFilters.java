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

/** Creates and caches {@link IndexingFilter} implementing plugins.*/
public class IndexingFilters {

  public static final String INDEXINGFILTER_ORDER = "indexingfilter.order";

  public final static Logger LOG = LoggerFactory.getLogger(IndexingFilters.class);

  private IndexingFilter[] indexingFilters;

  public IndexingFilters(Configuration conf) {
    /* Get indexingfilter.order property */
    String order = conf.get(INDEXINGFILTER_ORDER);
    ObjectCache objectCache = ObjectCache.get(conf);
    this.indexingFilters = (IndexingFilter[]) objectCache
        .getObject(IndexingFilter.class.getName());
    if (this.indexingFilters == null) {
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
            IndexingFilter.X_POINT_ID);
        if (point == null)
          throw new RuntimeException(IndexingFilter.X_POINT_ID + " not found.");
        Extension[] extensions = point.getExtensions();
        HashMap<String, IndexingFilter> filterMap =
          new HashMap<String, IndexingFilter>();
        for (int i = 0; i < extensions.length; i++) {
          Extension extension = extensions[i];
          IndexingFilter filter = (IndexingFilter) extension
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
          objectCache.setObject(IndexingFilter.class.getName(),
              filterMap.values().toArray(
                  new IndexingFilter[0]));
          /* Otherwise run the filters in the required order */
        } else {
          ArrayList<IndexingFilter> filters = new ArrayList<IndexingFilter>();
          for (int i = 0; i < orderedFilters.length; i++) {
            IndexingFilter filter = filterMap.get(orderedFilters[i]);
            if (filter != null) {
              filters.add(filter);
            }
          }
          objectCache.setObject(IndexingFilter.class.getName(), filters
              .toArray(new IndexingFilter[filters.size()]));
        }
      } catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }
      this.indexingFilters = (IndexingFilter[]) objectCache
          .getObject(IndexingFilter.class.getName());
    }
  }
  /** Run all defined filters. */
  public NutchDocument filter(NutchDocument doc, String url, WebPage page)
  throws IndexingException {
    for (IndexingFilter indexingFilter : indexingFilters) {
      doc = indexingFilter.filter(doc, url, page);
      // break the loop if an indexing filter discards the doc
      if (doc == null) return null;
    }

    return doc;
  }

  /**
   * Gets all the fields for a given {@link WebPage}
   * Many datastores need to setup the mapreduce job by specifying the fields
   * needed. All extensions that work on WebPage are able to specify what fields
   * they need.
   */
  public Collection<WebPage.Field> getFields() {
    Collection<WebPage.Field> columns = new HashSet<WebPage.Field>();
    for (IndexingFilter indexingFilter : indexingFilters) {
      Collection<WebPage.Field> fields = indexingFilter.getFields();
      if (fields != null) {
        columns.addAll(fields);
      }
    }
    return columns;
  }

}
