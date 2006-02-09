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

package org.apache.nutch.segment;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.util.NutchConfiguration;

/** Dump the content of a segment. */
public class SegmentReader extends Configured implements Reducer {

  public static final String DIR_NAME = "segdump";

  public static final Logger LOG =
    LogFormatter.getLogger(SegmentReader.class.getName());

  long recNo = 0L;

  /** Wraps inputs in an {@link ObjectWritable}, to permit merging different
   * types in reduce. */
  public static class InputFormat extends SequenceFileInputFormat {
    public RecordReader getRecordReader(FileSystem fs, FileSplit split,
                                        JobConf job, Reporter reporter)
      throws IOException {
      reporter.setStatus(split.toString());

      return new SequenceFileRecordReader(job, split) {
          public synchronized boolean next(Writable key, Writable value)
            throws IOException {
            ObjectWritable wrapper = (ObjectWritable)value;
            try {
              wrapper.set(getValueClass().newInstance());
            } catch (Exception e) {
              throw new IOException(e.toString());
            }
            return super.next(key, (Writable)wrapper.get());
          }
        };
    }
  }

  /** Implements a text output format*/
  public static class TextOutputFormat
  implements org.apache.hadoop.mapred.OutputFormat {
  public RecordWriter getRecordWriter(final FileSystem fs, JobConf job,
                                      String name) throws IOException {

   final File segmentDumpFile =
     new File(new File(job.getOutputDir(), SegmentReader.DIR_NAME), name);

   // Get the old copy out of the way
   fs.delete(segmentDumpFile);

   final PrintStream printStream = new PrintStream(fs.create(segmentDumpFile));
   return new RecordWriter() {
       public synchronized void write(WritableComparable key, Writable value)
         throws IOException {
         ObjectWritable writable = (ObjectWritable)value;
         printStream.println((String)writable.get());
       }
       public synchronized void close(Reporter reporter) throws IOException {
         printStream.close();
       }
     };
  }
}

  public SegmentReader() { 
      super(null); 
  }

  public SegmentReader(Configuration conf) {
    super(conf);
  }

  public void configure(JobConf job) {}

  public void close() {}

  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter)
    throws IOException {
    StringBuffer dump = new StringBuffer();
    
    dump.append("\nRecno:: ").append(recNo++).append("\n");
    dump.append("URL: " + key.toString() + "\n");
    while (values.hasNext()) {
      Object value = ((ObjectWritable)values.next()).get(); // unwrap
      if (value instanceof CrawlDatum) {
        dump.append("\nCrawlDatum::\n").append(((CrawlDatum)value).toString());  
      } else if (value instanceof Content) {
          dump.append("\nContent::\n").append(((Content)value).toString());
      } else if (value instanceof ParseData) {
          dump.append("\nParseData::\n").append(((ParseData)value).toString());
      } else if (value instanceof ParseText) {
          dump.append("\nParseText::\n").append(((ParseText)value).toString());
      } else {
        LOG.warning("Unrecognized type: " + value.getClass());
      }
    }
    output.collect(key, new ObjectWritable(dump.toString()));
  }

  public void reader(File segment) throws IOException {
    LOG.info("Reader: segment: " + segment);

    JobConf job = new JobConf(getConf());

    job.addInputDir(new File(segment, CrawlDatum.GENERATE_DIR_NAME));
    job.addInputDir(new File(segment, CrawlDatum.FETCH_DIR_NAME));
    job.addInputDir(new File(segment, CrawlDatum.PARSE_DIR_NAME));
    job.addInputDir(new File(segment, Content.DIR_NAME));
    job.addInputDir(new File(segment, ParseData.DIR_NAME));
    job.addInputDir(new File(segment, ParseText.DIR_NAME));

    job.setInputFormat(InputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(ObjectWritable.class);

    job.setReducerClass(SegmentReader.class);
    
    job.setOutputDir(segment);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(ObjectWritable.class);

    JobClient.runJob(job);
    
    // concatenate the output
    FileSystem fs = FileSystem.get(job);
    File directory = new File(job.getOutputDir(), SegmentReader.DIR_NAME);
    File dumpFile = new File(directory, job.get("segment.dump.dir", "dump"));

    // remove the old file 
    fs.delete(dumpFile);
    File[] files = fs.listFiles(directory);
    
    PrintWriter writer = null;
    int currentReccordNumber = 0;
    if (files.length > 0) {
        writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(fs.create(dumpFile))));
        try {
            for (int i = 0 ; i < files.length; i++) {
                File partFile = (File)files[i];
                try {
                    currentReccordNumber = append(fs, job, partFile, writer, currentReccordNumber);
                } catch (IOException exception) {
                    LOG.warning("Couldn't copy the content of " + partFile.toString() + " into " + dumpFile.toString());
                    LOG.warning(exception.getMessage());
                }
            }
        }
        finally {
            writer.close();
        }
    }
    LOG.info("Reader: done");
  }

  /** Appends two files and updates the Recno counter*/
  private int append(FileSystem fs, Configuration conf, File src, PrintWriter writer, int currentReccordNumber) throws IOException {
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(src)));
      try {
          String line = reader.readLine();
          while(line != null) {
              if (line.startsWith("Recno:: ")) {
                  line = "Recno:: " + currentReccordNumber++;
              }
              writer.println(line);
              line = reader.readLine();
          }
          return currentReccordNumber;
      } finally {
          reader.close();
      }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = NutchConfiguration.create();
    SegmentReader segmentReader = new SegmentReader(conf);

    String usage = "Usage: SegmentReader <segment>";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
    segmentReader.reader(new File(args[0]));
  }
}
