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
package org.apache.nutch.net;

import org.apache.hadoop.conf.Configurable;

import org.apache.nutch.plugin.Pluggable;

/**
 * Interface used to limit which URLs enter Nutch. Used per default by injector,
 * fetcher and parser for all URLs seen first (seeds, outlinks, redirects). URL
 * filters can be optionally enabled for many more Nutch tools.
 */
public interface URLFilter extends Pluggable, Configurable {

  /** The name of the extension point. */
  public final static String X_POINT_ID = URLFilter.class.getName();

  /**
   * Interface for a filter that transforms a URL: it can pass the original URL
   * through or "delete" the URL by returning null
   * 
   * @param urlString
   *          the URL string the filter is applied on
   * @return the original URL string if the URL is accepted by the filter or
   *         null in case the URL is rejected
   */
  public String filter(String urlString);
}
