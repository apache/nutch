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
    String indexName = "index-" + context.getTaskAttemptID().toString();
    writers.open(conf, indexName);
    LOG.info(writers.describe());

    return new RecordWriter<Text, NutchIndexAction>() {

      @Override
      public void close(TaskAttemptContext context) throws IOException {

        // do the commits once and for all the reducers in one go
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