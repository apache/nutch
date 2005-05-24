/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.nutch.io.*;
import org.apache.nutch.mapred.*;

/** Partition urls by hostname. */
public class PartitionUrlByHost implements Partitioner {
  private int seed;

  public void configure(JobConf job) {
    seed = job.getInt("partition.url.by.host.seed", 0);
  }
  
  /** Hash by hostname. */
  public int getPartition(WritableComparable key, Writable value,
                          int numReduceTasks) {
    String urlString = ((UTF8)key).toString();
    URL url = null;
    try {
      url = new URL(urlString);
    } catch (MalformedURLException e) {
    }
    int hashCode = (url==null ? urlString : url.getHost()).hashCode();

    // make hosts wind up in different partitions on different runs
    hashCode ^= seed;

    return (hashCode & Integer.MAX_VALUE) % numReduceTasks;
  }
  
}


