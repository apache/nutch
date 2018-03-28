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

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.ObjectCache;

/** Creates and caches {@link DuplicateFilter} implementing plugins. */
public class DuplicateFilters {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private DuplicateFilter[] duplicateFilters;

  public DuplicateFilters(Configuration conf) {
    ObjectCache objectCache = ObjectCache.get(conf);
    this.duplicateFilters = (DuplicateFilter[]) objectCache
        .getObject(DuplicateFilter.class.getName());
    if (this.duplicateFilters == null) {
      try {
        ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(
            DuplicateFilter.X_POINT_ID);
        if (point == null)
          throw new RuntimeException(DuplicateFilter.X_POINT_ID + " not found.");
        Extension[] extensions = point.getExtensions();
        HashMap<String, DuplicateFilter> filterMap = new HashMap<String, DuplicateFilter>();
        for (int i = 0; i < extensions.length; i++) {
          Extension extension = extensions[i];
          DuplicateFilter filter = (DuplicateFilter) extension
              .getExtensionInstance();
          LOG.info("Adding " + filter.getClass().getName());
          if (!filterMap.containsKey(filter.getClass().getName())) {
            filterMap.put(filter.getClass().getName(), filter);
          }
        }
        objectCache.setObject(DuplicateFilter.class.getName(), filterMap
            .values().toArray(new DuplicateFilter[0]));
      } catch (PluginRuntimeException e) {
        throw new RuntimeException(e);
      }
      this.duplicateFilters = (DuplicateFilter[]) objectCache
          .getObject(DuplicateFilter.class.getName());
    }
  }

  /** Run all defined filters. */
  public CharSequence filter(List<CharSequence> duplicates, Iterable<WebPage> webPages) {
    CharSequence original;
    for (DuplicateFilter duplicateFilter : duplicateFilters) {
      if ((original = duplicateFilter.filter(duplicates, webPages)) != null) {
        return original;
      }
    }

    return null;
  }

}
