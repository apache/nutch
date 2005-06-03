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

import org.apache.nutch.protocol.Content;
import org.apache.nutch.plugin.*;

import org.w3c.dom.DocumentFragment;

/** Creates and caches {@link HtmlParseFilter} implementing plugins.*/
public class HtmlParseFilters {

  private static final HtmlParseFilter[] CACHE;
  static {
    try {
      ExtensionPoint point = PluginRepository.getInstance()
        .getExtensionPoint(HtmlParseFilter.X_POINT_ID);
      if (point == null)
        throw new RuntimeException(HtmlParseFilter.X_POINT_ID+" not found.");
      Extension[] extensions = point.getExtentens();
      CACHE = new HtmlParseFilter[extensions.length];
      for (int i = 0; i < extensions.length; i++) {
        Extension extension = extensions[i];
        CACHE[i] = (HtmlParseFilter)extension.getExtensionInstance();
      }
    } catch (PluginRuntimeException e) {
      throw new RuntimeException(e);
    }
  }

  private  HtmlParseFilters() {}                  // no public ctor

  /** Run all defined filters. */
  public static Parse filter(Content content, Parse parse, HTMLMetaTags metaTags, DocumentFragment doc) {

    for (int i = 0 ; i < CACHE.length; i++) {
      parse = CACHE[i].filter(content, parse, metaTags, doc);
      if (!parse.getData().getStatus().isSuccess()) break;
    }

    return parse;
  }
}
