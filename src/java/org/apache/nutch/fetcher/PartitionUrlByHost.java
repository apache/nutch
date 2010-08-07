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
