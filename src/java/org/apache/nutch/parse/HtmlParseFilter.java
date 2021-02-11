/*
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

import org.w3c.dom.DocumentFragment;

import org.apache.hadoop.conf.Configurable;

import org.apache.nutch.plugin.Pluggable;
import org.apache.nutch.protocol.Content;

/**
 * Extension point for DOM-based HTML parsers. Permits one to add additional
 * metadata to HTML parses. All plugins found which implement this extension
 * point are run sequentially on the parse.
 */
public interface HtmlParseFilter extends Pluggable, Configurable {
  /** The name of the extension point. */
  final static String X_POINT_ID = HtmlParseFilter.class.getName();

  /**
   * Adds metadata or otherwise modifies a parse of HTML content, given the DOM
   * tree of a page.
   * @param content the {@link Content} for a given response
   * @param parseResult the result of running on or more 
   * {@link Parser}'s on the content. 
   * @see Parser#getParse(Content)
   * @param metaTags a populated {@link HTMLMetaTags} object
   * @param doc a {@link DocumentFragment} (DOM) which can be processed in
   * the filtering process.
   * @return a filtered {@link ParseResult}
   */
  ParseResult filter(Content content, ParseResult parseResult,
      HTMLMetaTags metaTags, DocumentFragment doc);
}
