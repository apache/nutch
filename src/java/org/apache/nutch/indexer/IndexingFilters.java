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
import org.apache.nutch.util.NutchConf;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.io.UTF8;

/** Creates and caches {@link IndexingFilter} implementing plugins.*/
public class IndexingFilters {

  private IndexingFilter[] indexingFilters;

  public IndexingFilters(NutchConf nutchConf) {
      this.indexingFilters =(IndexingFilter[]) nutchConf.getObject(IndexingFilter.class.getName()); 
      if (this.indexingFilters == null) {
            try {
                ExtensionPoint point = nutchConf.getPluginRepository().getExtensionPoint(IndexingFilter.X_POINT_ID);
                if (point == null)
                    throw new RuntimeException(IndexingFilter.X_POINT_ID + " not found.");
                Extension[] extensions = point.getExtensions();
                HashMap filterMap = new HashMap();
                for (int i = 0; i < extensions.length; i++) {
                    Extension extension = extensions[i];
                    IndexingFilter filter = (IndexingFilter) extension.getExtensionInstance();
                    System.out.println("-adding " + filter.getClass().getName());
                    if (!filterMap.containsKey(filter.getClass().getName())) {
                        filterMap.put(filter.getClass().getName(), filter);
                    }
                }
                nutchConf.setObject(IndexingFilter.class.getName(), (IndexingFilter[]) filterMap.values().toArray(new IndexingFilter[0]));
            } catch (PluginRuntimeException e) {
                throw new RuntimeException(e);
            }
            this.indexingFilters =(IndexingFilter[]) nutchConf.getObject(IndexingFilter.class.getName());
        }
  }                  

  /** Run all defined filters. */
  public Document filter(Document doc, Parse parse, UTF8 url, CrawlDatum datum, Inlinks inlinks)
    throws IndexingException {
    for (int i = 0; i < this.indexingFilters.length; i++) {
      doc = this.indexingFilters[i].filter(doc, parse, url, datum, inlinks);
    }

    return doc;
  }
}
