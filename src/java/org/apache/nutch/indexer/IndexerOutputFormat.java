/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 */

package org.apache.nutch.indexer;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexerOutputFormat extends OutputFormat<Text, NutchIndexAction> {

  private static final Logger LOG =
      LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public RecordWriter<Text, NutchIndexAction> getRecordWriter(
      TaskAttemptContext context) throws IOException {

    Configuration conf = context.getConfiguration();
    final IndexWriters writers = IndexWriters.get(conf);

    // open writers (no temporary file output anymore)
    writers.open(conf, "index");
    LOG.info(writers.describe());

    return new RecordWriter<Text, NutchIndexAction>() {

      @Override
      public void close(TaskAttemptContext context) throws IOException {

        boolean noCommit =
            conf.getBoolean(IndexerMapReduce.INDEXER_NO_COMMIT, false);

        if (!noCommit) {
          writers.commit();
        }

        writers.close();
      }

      @Override
      public void write(Text key, NutchIndexAction indexAction)
          throws IOException {

        if (indexAction.action == NutchIndexAction.ADD) {
          writers.write(indexAction.doc);

        } else if (indexAction.action == NutchIndexAction.DELETE) {
          writers.delete(key.toString());
        }
      }
    };
  }

  @Override
  public void checkOutputSpecs(JobContext context)
      throws IOException, InterruptedException {
    // No output specs required since we don't write files
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {

    return new OutputCommitter() {

      @Override
      public void setupJob(JobContext jobContext) {}

      @Override
      public void setupTask(TaskAttemptContext taskContext) {}

      @Override
      public boolean needsTaskCommit(TaskAttemptContext taskContext) {
        return false;
      }

      @Override
      public void commitTask(TaskAttemptContext taskContext) {}

      @Override
      public void abortTask(TaskAttemptContext taskContext) {}
    };
  }
}