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

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndexerOutputFormat
extends FileOutputFormat<String, NutchDocument> {

  @Override
  public RecordWriter<String, NutchDocument> getRecordWriter(
      TaskAttemptContext job) throws IOException, InterruptedException {

    final NutchIndexWriter[] writers =
      NutchIndexWriterFactory.getNutchIndexWriters(job.getConfiguration());

    for (final NutchIndexWriter writer : writers) {
      writer.open(job, FileOutputFormat.getUniqueFile(job, "part", ""));
    }

    return new RecordWriter<String, NutchDocument>() {

      @Override
      public void write(String key, NutchDocument doc) throws IOException {
        for (final NutchIndexWriter writer : writers) {
          writer.write(doc);
        }
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException,
      InterruptedException {
        for (final NutchIndexWriter writer : writers) {
          writer.close();
        }
      }
    };
  }
}
