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
package org.commoncrawl.util;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarcOutputFormat extends FileOutputFormat<Text, WarcCapture> {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private OutputCommitter committer;

  public RecordWriter<Text, WarcCapture> getRecordWriter(
      TaskAttemptContext context) throws IOException {

    TaskID taskid = context.getTaskAttemptID().getTaskID();
    int partition = taskid.getId();
    Configuration conf = context.getConfiguration();
    Path outputPath = getOutputPath(context);

    return getRecordWriter(conf, outputPath, partition);
  }

  public RecordWriter<Text, WarcCapture> getRecordWriter(
      Configuration conf, Path outputPath, int partition) throws IOException {

    LOG.info("Partition: " + partition);

    String warcOutputPath = conf.get("warc.export.path");
    if (warcOutputPath != null) {
      LOG.info("Writing WARC output to {} as configured by warc.export.path",
          warcOutputPath);
      outputPath = new Path(warcOutputPath);
    }

    return new WarcRecordWriter(conf, outputPath, partition);
  }

  @Override
  public synchronized OutputCommitter getOutputCommitter(
      TaskAttemptContext context) throws java.io.IOException {
    if (committer == null) {
      Path output = getOutputPath(context);

      String scheme = output.getFileSystem(context.getConfiguration()).getScheme();
      if (scheme.startsWith("s3")) {
        committer = new NullOutputCommitter();
      } else {
        committer = super.getOutputCommitter(context);
      }
    }
    return committer;
  }

  @Override
  public void checkOutputSpecs(JobContext job)
      throws FileAlreadyExistsException, IOException {
    // Ensure that the output directory is set and not already there
    Path outDir = getOutputPath(job);
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set.");
    }

    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(job.getCredentials(),
        new Path[] { outDir }, job.getConfiguration());
  }

}