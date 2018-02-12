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

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partition urls by host, domain name or IP depending on the value of the
 * parameter 'partition.url.mode' which can be 'byHost', 'byDomain' or 'byIP'
 */
public class URLPartitioner implements Partitioner<Text, Writable> {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles
      .lookup().lookupClass());

  public static final String PARTITION_MODE_KEY = "partition.url.mode";

  public static final String PARTITION_MODE_HOST = "byHost";
  public static final String PARTITION_MODE_DOMAIN = "byDomain";
  public static final String PARTITION_MODE_IP = "byIP";

  private int seed;
  private URLNormalizers normalizers;
  private String mode = PARTITION_MODE_HOST;

  public void configure(JobConf job) {
    seed = job.getInt("partition.url.seed", 0);
    mode = job.get(PARTITION_MODE_KEY, PARTITION_MODE_HOST);
    // check that the mode is known
    if (!mode.equals(PARTITION_MODE_IP) && !mode.equals(PARTITION_MODE_DOMAIN)
        && !mode.equals(PARTITION_MODE_HOST)) {
      LOG.error("Unknown partition mode : " + mode + " - forcing to byHost");
      mode = PARTITION_MODE_HOST;
    }
    normalizers = new URLNormalizers(job, URLNormalizers.SCOPE_PARTITION);
  }

  public void close() {
  }

  public String getNormalizedURLRoot(Text key) {
    URLNormalizers normalizers = this.normalizers;

    String urlString = key.toString();
    URL url = null;
    try {
      urlString = URLUtil.normalizeURL(urlString, normalizers);
      url = new URL(urlString);
    } catch (MalformedURLException e) {
      LOG.warn("Malformed URL: '" + urlString + "'");
      return urlString;
    }

    try {
      urlString = URLUtil.getUrlRootByMode(url, mode);
    } catch (UnknownHostException e) {
      Generator.LOG.info("Couldn't find IP for host: " + url.getHost());
    }
    return urlString;
  }

  /** Hash by host or domain name or IP address. */
  public int getPartition(Text key, Writable value, int numReduceTasks) {
    String partitionKey;

    if (key.toString().isEmpty()) {
      partitionKey = value.toString();
    } else {
      partitionKey = getNormalizedURLRoot(key);
    }

    int hashCode = partitionKey.hashCode();
    // make hosts wind up in different partitions on different runs
    hashCode ^= seed;

    return (hashCode & Integer.MAX_VALUE) % numReduceTasks;
  }

}
