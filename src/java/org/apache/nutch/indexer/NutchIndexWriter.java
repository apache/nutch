package org.apache.nutch.indexer;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;

public interface NutchIndexWriter {
  public void open(JobConf job, String name) throws IOException;

  public void write(NutchDocument doc) throws IOException;

  public void close() throws IOException;

}
