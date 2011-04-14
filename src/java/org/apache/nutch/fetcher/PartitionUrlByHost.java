/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.fetcher;

import java.net.URL;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.nutch.util.TableUtil;

public class PartitionUrlByHost
extends Partitioner<IntWritable, FetchEntry> {

  @Override
  public int getPartition(IntWritable key,
      FetchEntry value, int numPartitions) {
    String urlString = TableUtil.unreverseUrl(value.getKey());

    URL url = null;

    int hashCode = (url==null ? urlString : url.getHost()).hashCode();

    return (hashCode & Integer.MAX_VALUE) % numPartitions;
  }
}
