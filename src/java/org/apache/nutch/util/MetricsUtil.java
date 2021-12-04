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
package org.apache.nutch.util;

import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;

import lombok.NonNull;

/**
 * Utility functionality related to Nutch metrics collection. Nutch uses 
 * the <a href="https://github.com/statsd/statsd">StatsD standard</a> to 
 * aggregate and summarize application metrics.
 * @see <a href="https://cwiki.apache.org/confluence/display/NUTCH/Metrics">Nutch Metrics</a>
 * for more information about Nutch metrics specifically
 * @see <a href="https://www.datadoghq.com/blog/statsd/">StatsD, what it is and how it can help you</a>
 * for much more information on StatsD
 * @see <a href="https://issues.apache.org/jira/browse/NUTCH-2909">NUTCH-2909 Establish a metrics naming convention</a>
 */
public class MetricsUtil {

  /**
   * Create a new {@link NonBlockingStatsDClientBuilder} with <b>required</b> <code>host</code>, 
   * <code>port</code> and <code>prefix</code> parameters. These are defined within 
   * <code>nutch-default.xml</code>) so can be obtained from <code>statsd.hostname</code>,
   * <code>statsd.port</code> and <code>statsd.prefix</code> respectively within 
   * {@link org.apache.hadoop.conf.Configuration}.
   * @param host StatsD server host listening for messages.
   * @param port StatsD server port listening for messages.
   * @param prefix Global prefix to use for sending Nutch stats to StatsD.
   * @return an instantiated {@link NonBlockingStatsDClientBuilder}
   */
  public static StatsDClient createNonBlockingStatsDClient(@NonNull String host, @NonNull int port, @NonNull String prefix) {
    return new NonBlockingStatsDClientBuilder()
            .hostname(host)
            .port(port)
            .prefix(prefix)
            .build();
  }

}
