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

package org.apache.nutch.crawl;

import java.io.IOException;
import java.io.File;

import org.apache.nutch.fs.NutchFileSystem;

import org.apache.nutch.io.MapFile;
import org.apache.nutch.io.WritableComparable;
import org.apache.nutch.io.Writable;
import org.apache.nutch.io.UTF8;

import org.apache.nutch.mapred.OutputFormat;
import org.apache.nutch.mapred.RecordWriter;
import org.apache.nutch.mapred.JobConf;

import org.apache.nutch.protocol.Content;

/** Splits FetcherOutput entries into multiple map files. */
public class FetcherOutputFormat implements OutputFormat {

  public RecordWriter getRecordWriter(NutchFileSystem fs, JobConf job,
                                      String name) throws IOException {

    File dir = new File(job.getOutputDir(), name);

    final MapFile.Writer crawlOut =
      new MapFile.Writer(fs, new File(dir, CrawlDatum.DIR_NAME).toString(),
                         UTF8.class, CrawlDatum.class);
    
    final MapFile.Writer contentOut =
      new MapFile.Writer(fs, new File(dir, Content.DIR_NAME).toString(),
                         UTF8.class, Content.class);

    return new RecordWriter() {

        public void write(WritableComparable key, Writable value)
          throws IOException {

          FetcherOutput fo = (FetcherOutput)value;
          
          crawlOut.append(key, fo.getCrawlDatum());
          contentOut.append(key, fo.getContent());
        }

        public void close() throws IOException {
          crawlOut.close();
          contentOut.close();
        }

      };

  }      
}

