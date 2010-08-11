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

import org.apache.hadoop.conf.Configurable;
import org.apache.nutch.plugin.FieldPluggable;
import org.apache.nutch.storage.WebPage;
import org.w3c.dom.DocumentFragment;


/** Extension point for DOM-based parsers.  Permits one to add additional
 * metadata to parses provided by the html or tika plugins.  All plugins found which implement this extension
 * point are run sequentially on the parse.
 */
public interface ParseFilter extends FieldPluggable, Configurable {
  /** The name of the extension point. */
  final static String X_POINT_ID = ParseFilter.class.getName();

  /** Adds metadata or otherwise modifies a parse, given
   * the DOM tree of a page. */
  Parse filter(String url, WebPage page, Parse parse,
                    HTMLMetaTags metaTags, DocumentFragment doc);

}
