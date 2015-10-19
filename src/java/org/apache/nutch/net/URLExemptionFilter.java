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

package org.apache.nutch.net;

//Hadoop
import org.apache.hadoop.conf.Configurable;
// Nutch
import org.apache.nutch.plugin.Pluggable;

/**
 * Interface used to allow exemptions to external domain resources by overriding <code>db.ignore.external.links</code>.
 * This is useful when the crawl is focused to a domain but resources like images are hosted on CDN.
 */

public interface URLExemptionFilter extends Pluggable, Configurable{

  /** The name of the extension point. */
  public final static String X_POINT_ID = URLExemptionFilter.class.getName();

  /**
   * Checks if toUrl is exempted when the ignore external is enabled
   * @param fromUrl : the source url which generated the outlink
   * @param toUrl : the destination url which needs to be checked for exemption
   * @return true when toUrl is exempted from dbIgnore
   */
  public boolean filter(String fromUrl, String toUrl);

}
