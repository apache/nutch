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

package org.apache.nutch.crawl;

import java.net.URL;
import java.net.MalformedURLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.nutch.net.URLNormalizers;

/** Partition urls by hostname. */
public class PartitionUrlByHost implements Partitioner<Text, Writable> {
  private static final Log LOG = LogFactory.getLog(PartitionUrlByHost.class);
  
  private int seed;
  private URLNormalizers normalizers;

  public void configure(JobConf job) {
    seed = job.getInt("partition.url.by.host.seed", 0);
    normalizers = new URLNormalizers(job, URLNormalizers.SCOPE_PARTITION);
  }
  
  public void close() {}

  /** Hash by hostname. */
  public int getPartition(Text key, Writable value,
                          int numReduceTasks) {
    String urlString = key.toString();
    try {
      urlString = normalizers.normalize(urlString, URLNormalizers.SCOPE_PARTITION);
    } catch (Exception e) {
      LOG.warn("Malformed URL: '" + urlString + "'");
    }
    URL url = null;
    try {
      url = new URL(urlString);
    } catch (MalformedURLException e) {
      LOG.warn("Malformed URL: '" + urlString + "'");
    }
    int hashCode = (url==null ? urlString : url.getHost()).hashCode();

    // make hosts wind up in different partitions on different runs
    hashCode ^= seed;

    return (hashCode & Integer.MAX_VALUE) % numReduceTasks;
  }
  
}


