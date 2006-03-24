/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.File;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.UTF8;

import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import org.apache.nutch.parse.ParseOutputFormat;
import org.apache.nutch.protocol.Content;

/** Splits FetcherOutput entries into multiple map files. */
public class FetcherOutputFormat implements OutputFormat {

  public void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException {
    if (fs.exists(new File(job.getOutputDir(), CrawlDatum.FETCH_DIR_NAME)))
      throw new IOException("Segment already fetched!");
  }

  public RecordWriter getRecordWriter(final FileSystem fs,
                                      final JobConf job,
                                      final String name) throws IOException {

    final File fetch =
      new File(new File(job.getOutputDir(), CrawlDatum.FETCH_DIR_NAME), name);
    final File content =
      new File(new File(job.getOutputDir(), Content.DIR_NAME), name);

    final MapFile.Writer fetchOut =
      new MapFile.Writer(fs, fetch.toString(), UTF8.class, CrawlDatum.class);
    
    return new RecordWriter() {
        private MapFile.Writer contentOut;
        private RecordWriter parseOut;

        {
          if (Fetcher.isStoringContent(job)) {
            contentOut = new MapFile.Writer(fs, content.toString(),
                                            UTF8.class, Content.class);
          }

          if (Fetcher.isParsing(job)) {
            parseOut = new ParseOutputFormat().getRecordWriter(fs, job, name);
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

