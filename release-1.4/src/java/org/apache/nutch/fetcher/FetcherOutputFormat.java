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
import org.apache.nutch.crawl.NutchWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;

import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseOutputFormat;
import org.apache.nutch.protocol.Content;

/** Splits FetcherOutput entries into multiple map files. */
public class FetcherOutputFormat implements OutputFormat<Text, NutchWritable> {

  public void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException {
    Path out = FileOutputFormat.getOutputPath(job);
    if ((out == null) && (job.getNumReduceTasks() != 0)) {
    	throw new InvalidJobConfException(
    			"Output directory not set in JobConf.");
    }
    if (fs == null) {
    	fs = out.getFileSystem(job);
    }
    if (fs.exists(new Path(out, CrawlDatum.FETCH_DIR_NAME)))
    	throw new IOException("Segment already fetched!");
  }

  public RecordWriter<Text, NutchWritable> getRecordWriter(final FileSystem fs,
                                      final JobConf job,
                                      final String name,
                                      final Progressable progress) throws IOException {

    Path out = FileOutputFormat.getOutputPath(job);
    final Path fetch =
      new Path(new Path(out, CrawlDatum.FETCH_DIR_NAME), name);
    final Path content =
      new Path(new Path(out, Content.DIR_NAME), name);
    
    final CompressionType compType = SequenceFileOutputFormat.getOutputCompressionType(job);

    final MapFile.Writer fetchOut =
      new MapFile.Writer(job, fs, fetch.toString(), Text.class, CrawlDatum.class,
          compType, progress);
    
    return new RecordWriter<Text, NutchWritable>() {
        private MapFile.Writer contentOut;
        private RecordWriter<Text, Parse> parseOut;

        {
          if (Fetcher.isStoringContent(job)) {
            contentOut = new MapFile.Writer(job, fs, content.toString(),
                                            Text.class, Content.class,
                                            compType, progress);
          }

          if (Fetcher.isParsing(job)) {
            parseOut = new ParseOutputFormat().getRecordWriter(fs, job, name, progress);
          }
        }

        public void write(Text key, NutchWritable value)
          throws IOException {

          Writable w = value.get();
          
          if (w instanceof CrawlDatum)
            fetchOut.append(key, w);
          else if (w instanceof Content)
            contentOut.append(key, w);
          else if (w instanceof Parse)
            parseOut.write(key, (Parse)w);
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

