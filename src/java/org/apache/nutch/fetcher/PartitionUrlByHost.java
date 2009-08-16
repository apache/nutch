package org.apache.nutch.fetcher;

import java.net.URL;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionUrlByHost
extends Partitioner<ImmutableBytesWritable, FetchEntry> {

  @Override
  public int getPartition(ImmutableBytesWritable key,
      FetchEntry value, int numPartitions) {
    String urlString = Bytes.toString(value.getKey().get());

    URL url = null;

    int hashCode = (url==null ? urlString : url.getHost()).hashCode();

    return (hashCode & Integer.MAX_VALUE) % numPartitions;
  }
}
