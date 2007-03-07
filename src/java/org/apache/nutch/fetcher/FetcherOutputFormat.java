/**
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

package org.apache.nutch.fetcher;

import java.io.IOException;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import org.apache.nutch.parse.ParseOutputFormat;
import org.apache.nutch.protocol.Content;

/** Splits FetcherOutput entries into multiple map files. */
public class FetcherOutputFormat implements OutputFormat {

  public void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException {
    if (fs.exists(new Path(job.getOutputPath(), CrawlDatum.FETCH_DIR_NAME)))
      throw new IOException("Segment already fetched!");
  }

  public RecordWriter getRecordWriter(final FileSystem fs,
                                      final JobConf job,
                                      final String name,
                                      final Progressable progress) throws IOException {

    final Path fetch =
      new Path(new Path(job.getOutputPath(), CrawlDatum.FETCH_DIR_NAME), name);
    final Path content =
      new Path(new Path(job.getOutputPath(), Content.DIR_NAME), name);

    final MapFile.Writer fetchOut =
      new MapFile.Writer(job, fs, fetch.toString(), Text.class, CrawlDatum.class);
    
    return new RecordWriter() {
        private MapFile.Writer contentOut;
        private RecordWriter parseOut;

        {
          if (Fetcher.isStoringContent(job)) {
            contentOut = new MapFile.Writer(job, fs, content.toString(),
                                            Text.class, Content.class);
          }

          if (Fetcher.isParsing(job)) {
            parseOut = new ParseOutputFormat().getRecordWriter(fs, job, name, null);
          }
        }

        public void write(WritableComparable key, Writable value)
          throws IOException {

          FetcherOutput fo = (FetcherOutput)value;
          
          fetchOut.append(key, fo.getCrawlDatum());

          if (fo.getContent() != null) {
            contentOut.append(key, fo.getContent());
          }

          if (fo.getParse() != null) {
            parseOut.write(key, fo.getParse());
          }

        }

        public void close(Reporter reporter) throws IOException {
          fetchOut.close();
          if (contentOut != null) {
            contentOut.close();
          }
          if (parseOut != null) {
            parseOut.close(reporter);
          }
        }

      };

  }      
}

