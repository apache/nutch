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

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.nutch.crawl.GeneratorJob.SelectorEntry;
import org.apache.nutch.fetcher.FetchEntry;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;

/**
 * Partition urls by host, domain name or IP depending on the value of the
 * parameter 'partition.url.mode' which can be 'byHost', 'byDomain' or 'byIP'
 */
public class URLPartitioner implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(URLPartitioner.class);

  public static final String PARTITION_MODE_KEY = "partition.url.mode";

  public static final String PARTITION_MODE_HOST = "byHost";
  public static final String PARTITION_MODE_DOMAIN = "byDomain";
  public static final String PARTITION_MODE_IP = "byIP";
  
  public static final String PARTITION_URL_SEED = "partition.url.seed";

  private Configuration conf;

  private int seed;
  private URLNormalizers normalizers;
  private String mode = PARTITION_MODE_HOST;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    seed = conf.getInt(PARTITION_URL_SEED, 0);
    mode = conf.get(PARTITION_MODE_KEY, PARTITION_MODE_HOST);
    // check that the mode is known
    if (!mode.equals(PARTITION_MODE_IP) && !mode.equals(PARTITION_MODE_DOMAIN)
        && !mode.equals(PARTITION_MODE_HOST)) {
      LOG.error("Unknown partition mode : " + mode + " - forcing to byHost");
      mode = PARTITION_MODE_HOST;
    }
    normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_PARTITION);
  }

  public int getPartition(String urlString, int numReduceTasks) {
    if (numReduceTasks == 1) {
      //this check can be removed when we use Hadoop with MAPREDUCE-1287
      return 0;
    }
    
    int hashCode;
    URL url = null;
    try {
      urlString = normalizers.normalize(urlString, URLNormalizers.SCOPE_PARTITION);
      hashCode = urlString.hashCode();
      url = new URL(urlString);
    } catch (MalformedURLException e) {
      LOG.warn("Malformed URL: '" + urlString + "'");
      hashCode = urlString.hashCode();
    }
    
    if (url != null) {
      if (mode.equals(PARTITION_MODE_HOST)) {
        hashCode = url.getHost().hashCode();
      } else if (mode.equals(PARTITION_MODE_DOMAIN)) {
        hashCode = URLUtil.getDomainName(url).hashCode();
      } else { // MODE IP
        try {
          InetAddress address = InetAddress.getByName(url.getHost());
          hashCode = address.getHostAddress().hashCode();
        } catch (UnknownHostException e) {
          GeneratorJob.LOG.info("Couldn't find IP for host: " + url.getHost());
        }
      }
    }
    
    // make hosts wind up in different partitions on different runs
    hashCode ^= seed;
    return (hashCode & Integer.MAX_VALUE) % numReduceTasks;
  }
  
  
  public static class SelectorEntryPartitioner 
      extends Partitioner<SelectorEntry, WebPage> implements Configurable {
    private URLPartitioner partitioner = new URLPartitioner();
    private Configuration conf;
    
    @Override
    public int getPartition(SelectorEntry selectorEntry, WebPage page, int numReduces) {
      return partitioner.getPartition(selectorEntry.url, numReduces);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf=conf;
      partitioner.setConf(conf);
    }
  }
  
  public static class FetchEntryPartitioner
      extends Partitioner<IntWritable, FetchEntry> implements Configurable {
    private URLPartitioner partitioner = new URLPartitioner();
    private Configuration conf;
    
    @Override
    public int getPartition(IntWritable intWritable, FetchEntry fetchEntry, int numReduces) {
      String key = fetchEntry.getKey();
      String url = TableUtil.unreverseUrl(key);
      return partitioner.getPartition(url, numReduces);
    }
    
    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf=conf;
      partitioner.setConf(conf);
    }
  }
  
}
