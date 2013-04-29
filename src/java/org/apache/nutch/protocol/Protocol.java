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

// Hadoop imports

import org.apache.hadoop.conf.Configurable;
import org.apache.nutch.plugin.FieldPluggable;
import org.apache.nutch.storage.WebPage;

import crawlercommons.robots.BaseRobotRules;

/** A retriever of url content.  Implemented by protocol extensions. */
public interface Protocol extends FieldPluggable, Configurable {
  /** The name of the extension point. */
  public final static String X_POINT_ID = Protocol.class.getName();

  /**
   * Property name. If in the current configuration this property is set to
   * true, protocol implementations should handle "politeness" limits
   * internally. If this is set to false, it is assumed that these limits are
   * enforced elsewhere, and protocol implementations should not enforce them
   * internally.
   */
  public final static String CHECK_BLOCKING = "protocol.plugin.check.blocking";

  /**
   * Property name. If in the current configuration this property is set to
   * true, protocol implementations should handle robot exclusion rules
   * internally. If this is set to false, it is assumed that these limits are
   * enforced elsewhere, and protocol implementations should not enforce them
   * internally.
   */
  public final static String CHECK_ROBOTS = "protocol.plugin.check.robots";

  /*
   * Returns the {@link Content} for a fetchlist entry.
   */
  ProtocolOutput getProtocolOutput(String url, WebPage page);

  /**
   * Retrieve robot rules applicable for this url.
   * @param url url to check
   * @param page
   * @return robot rules (specific for this url or default), never null
   */
  BaseRobotRules getRobotRules(String url, WebPage page);
}
