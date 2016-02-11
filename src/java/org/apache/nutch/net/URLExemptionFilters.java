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

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates and caches {@link URLExemptionFilter} implementing plugins. */
public class URLExemptionFilters {

  private static final Logger LOG = LoggerFactory.getLogger(URLExemptionFilters.class);

  private URLExemptionFilter[] filters;

  public URLExemptionFilters(Configuration conf) {
    Extension[] extensions = PluginRepository.get(conf)
        .getExtensionPoint(URLExemptionFilter.X_POINT_ID).getExtensions();
    filters = new URLExemptionFilter[extensions.length];
    for (int i = 0; i < extensions.length; i++) {
      try {
        filters[i] = (URLExemptionFilter) extensions[i].getExtensionInstance();
      } catch (PluginRuntimeException e) {
        throw new IllegalStateException(e);
      }
    }
    LOG.info("Found {} extensions at point:'{}'", filters.length,
        URLExemptionFilter.X_POINT_ID);
  }


  /** Run all defined filters. Assume logical AND. */
  public boolean isExempted(String fromUrl, String toUrl) {
    if (filters.length < 1) {
      //at least one filter should be on
      return false;
    }
    //validate from, to and filters
    boolean exempted = fromUrl != null && toUrl != null;
    //An URL is exempted when all the filters accept it to pass through
    for (int i = 0; i < this.filters.length && exempted; i++) {
      exempted = this.filters[i].filter(fromUrl, toUrl);
    }
    return exempted;
  }
}