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

package org.apache.nutch.protocol;

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.Text;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.plugin.Pluggable;

import crawlercommons.robots.BaseRobotRules;

/** A retriever of url content. Implemented by protocol extensions. */
public interface Protocol extends Pluggable, Configurable {
  /** The name of the extension point. */
  public final static String X_POINT_ID = Protocol.class.getName();

  /**
   * Returns the {@link Content} for a fetchlist entry.
   */
  ProtocolOutput getProtocolOutput(Text url, CrawlDatum datum);

  /**
   * Retrieve robot rules applicable for this URL.
   *
   * @param url
   *          URL to check
   * @param datum
   *          page datum
   * @param robotsTxtContent
   *          container to store responses when fetching the robots.txt file for
   *          debugging or archival purposes. Instead of a robots.txt file, it
   *          may include redirects or an error page (404, etc.). Response
   *          {@link Content} is appended to the passed list. If null is passed
   *          nothing is stored.
   * @return robot rules (specific for this URL or default), never null
   */
  BaseRobotRules getRobotRules(Text url, CrawlDatum datum,
      List<Content> robotsTxtContent);

}
