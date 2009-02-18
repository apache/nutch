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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class IndexerOutputFormat extends FileOutputFormat<Text, NutchDocument> {

  @Override
  public RecordWriter<Text, NutchDocument> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) throws IOException {
    final NutchIndexWriter[] writers =
      NutchIndexWriterFactory.getNutchIndexWriters(job);

    for (final NutchIndexWriter writer : writers) {
      writer.open(job, name);
    }
    return new RecordWriter<Text, NutchDocument>() {

      public void close(Reporter reporter) throws IOException {
        for (final NutchIndexWriter writer : writers) {
          writer.close();
        }
      }

      public void write(Text key, NutchDocument doc) throws IOException {
        for (final NutchIndexWriter writer : writers) {
          writer.write(doc);
        }
      }
    };
  }
}
