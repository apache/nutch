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

package org.apache.nutch.net.protocols;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtocolLogUtil implements Configurable {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static final String HTTP_LOG_SUPPRESSION = "http.log.exceptions.suppress.stack";

  private Configuration config;

  /**
   * Set of exceptions logged shortly without full Java stack trace, see
   * property <code>http.log.exceptions.suppress.stack</code>.
   */
  private Set<Class<? extends Throwable>> exceptionsLogShort = new HashSet<>();

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
    for (String exceptClassName : conf.getTrimmedStrings(HTTP_LOG_SUPPRESSION,
        "java.net.UnknownHostException", "java.net.NoRouteToHostException")) {
      Class<?> clazz = conf.getClassByNameOrNull(exceptClassName);
      if (clazz == null) {
        LOG.warn("Class {} configured for log stack suppression not found.",
            exceptClassName);
        continue;
      }
      if (!Throwable.class.isAssignableFrom(clazz)) {
        LOG.warn(
            "Class {} configured for log stack suppression does not extend Throwable.",
            exceptClassName);
        continue;
      }
      exceptionsLogShort.add(clazz.asSubclass(Throwable.class));
    }
  }

  /**
   * Return true if exception is configured to be logged as short message
   * without stack trace, usually done for frequent exceptions with obvious
   * reasons (e.g., UnknownHostException), configurable by
   * <code>http.log.exceptions.suppress.stack</code>
   */
  public boolean logShort(Throwable t) {
    if (exceptionsLogShort.contains(t.getClass())) {
      return true;
    }
    return false;
  }

}
