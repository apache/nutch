/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.HashMap;

import org.apache.lucene.document.Document;

import org.apache.nutch.plugin.*;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.fetcher.FetcherOutput;

/** Creates and caches {@link IndexingFilter} implementing plugins.*/
public class IndexingFilters {

  private static final IndexingFilter[] CACHE;
  static {
    try {
      ExtensionPoint point = PluginRepository.getInstance()
        .getExtensionPoint(IndexingFilter.X_POINT_ID);
      if (point == null)
        throw new RuntimeException(IndexingFilter.X_POINT_ID+" not found.");
      Extension[] extensions = point.getExtentens();
      HashMap filterMap = new HashMap();
      for (int i = 0; i < extensions.length; i++) {
        Extension extension = extensions[i];
        IndexingFilter filter = (IndexingFilter)extension.getExtensionInstance();
        if (!filterMap.containsKey(filter.getClass().getName())) {
        	filterMap.put(filter.getClass().getName(), filter);
        }
      }
      CACHE = (IndexingFilter[])filterMap.values().toArray(new IndexingFilter[0]);
    } catch (PluginRuntimeException e) {
      throw new RuntimeException(e);
    }
  }

  private  IndexingFilters() {}                  // no public ctor

  /** Run all defined filters. */
  public static Document filter(Document doc, Parse parse, FetcherOutput fo)
    throws IndexingException {

    for (int i = 0; i < CACHE.length; i++) {
      doc = CACHE[i].filter(doc, parse, fo);
    }

    return doc;
  }
}
