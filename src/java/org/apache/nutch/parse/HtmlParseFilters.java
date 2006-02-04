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

package org.apache.nutch.parse;

import java.util.HashMap;

import org.apache.nutch.protocol.Content;
import org.apache.nutch.plugin.*;
import org.apache.hadoop.conf.Configuration;

import org.w3c.dom.DocumentFragment;

/** Creates and caches {@link HtmlParseFilter} implementing plugins.*/
public class HtmlParseFilters {

  private HtmlParseFilter[] htmlParseFilters;

  public HtmlParseFilters(Configuration conf) {
        this.htmlParseFilters = (HtmlParseFilter[]) conf.getObject(HtmlParseFilter.class.getName());
        if (htmlParseFilters == null) {
            HashMap filters = new HashMap();
            try {
                ExtensionPoint point = PluginRepository.get(conf).getExtensionPoint(HtmlParseFilter.X_POINT_ID);
                if (point == null)
                    throw new RuntimeException(HtmlParseFilter.X_POINT_ID + " not found.");
                Extension[] extensions = point.getExtensions();
                for (int i = 0; i < extensions.length; i++) {
                    Extension extension = extensions[i];
                    HtmlParseFilter parseFilter = (HtmlParseFilter) extension.getExtensionInstance();
                    if (!filters.containsKey(parseFilter.getClass().getName())) {
                        filters.put(parseFilter.getClass().getName(), parseFilter);
                    }
                }
                HtmlParseFilter[] htmlParseFilters = (HtmlParseFilter[]) filters.values().toArray(new HtmlParseFilter[filters.size()]);
                conf.setObject(HtmlParseFilter.class.getName(), htmlParseFilters);
            } catch (PluginRuntimeException e) {
                throw new RuntimeException(e);
            }
            this.htmlParseFilters = (HtmlParseFilter[]) conf.getObject(HtmlParseFilter.class.getName());
        }
    }                  

  /** Run all defined filters. */
  public Parse filter(Content content, Parse parse, HTMLMetaTags metaTags, DocumentFragment doc) {

    for (int i = 0 ; i < this.htmlParseFilters.length; i++) {
      parse = this.htmlParseFilters[i].filter(content, parse, metaTags, doc);
      if (!parse.getData().getStatus().isSuccess()) break;
    }

    return parse;
  }
}
