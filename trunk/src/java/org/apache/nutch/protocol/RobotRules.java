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

import java.net.URL;

/**
 * This class holds the rules which were parsed from a robots.txt file, and can
 * test paths against those rules.
 */
public interface RobotRules {
  /**
   * Get expire time
   */
  public long getExpireTime();

  /**
   * Get Crawl-Delay, in milliseconds. This returns -1 if not set.
   */
  public long getCrawlDelay();

  /**
   * Returns <code>false</code> if the <code>robots.txt</code> file
   * prohibits us from accessing the given <code>url</code>, or
   * <code>true</code> otherwise.
   */
  public boolean isAllowed(URL url);

}
