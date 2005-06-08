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

import java.io.*;
import java.util.*;
import java.util.logging.*;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.net.*;
import org.apache.nutch.util.*;
import org.apache.nutch.mapred.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.analysis.*;
import org.apache.nutch.indexer.*;

import org.apache.lucene.index.*;
import org.apache.lucene.document.*;

/** Maintains an inverted link map, listing incoming links for each url. */
public class Indexer extends NutchConfigured implements Reducer {

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.crawl.Indexer");

  /** Wraps inputs in an {@link ObjectWritable}, to permit merging different
   * types in reduce. */
  public static class InputFormat extends SequenceFileInputFormat {
    public RecordReader getRecordReader(NutchFileSystem fs, FileSplit split,
                                        JobConf job) throws IOException {
      return new SequenceFileRecordReader(fs, split) {
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

  /** Unwrap Lucene Documents created by reduce and add them to an index. */
  public static class OutputFormat
    implements org.apache.nutch.mapred.OutputFormat {
    public RecordWriter getRecordWriter(final NutchFileSystem fs, JobConf job,
                                        String name) throws IOException {
      final File perm = new File(job.getOutputDir(), name);
      final File temp = new File(job.getLocalDir(), "index-"
                                 +Integer.toString(new Random().nextInt()));

      fs.delete(perm);                            // delete old, if any

      final IndexWriter writer =                  // build locally first
        new IndexWriter(fs.startLocalOutput(perm, temp),
                        new NutchDocumentAnalyzer(), true);

      writer.mergeFactor = job.getInt("indexer.mergeFactor", 10);
      writer.minMergeDocs = job.getInt("indexer.minMergeDocs", 100);
      writer.maxMergeDocs =
        job.getInt("indexer.maxMergeDocs", Integer.MAX_VALUE);
      writer.setTermIndexInterval
        (job.getInt("indexer.termIndexInterval", 128));
      writer.maxFieldLength = job.getInt("indexer.max.tokens", 10000);

      return new RecordWriter() {

          public void write(WritableComparable key, Writable value)
            throws IOException {                  // unwrap & index doc
            writer.addDocument((Document)((ObjectWritable)value).get());
          }
          
          public void close() throws IOException {
            LOG.info("Optimizing index.");        // optimize & close index
            writer.optimize();
            writer.close();
            fs.completeLocalOutput(perm, temp);   // copy to ndfs
          }
        };
    }
  }

  public Indexer() {
    super(null);
  }

  /** Construct an Indexer. */
  public Indexer(NutchConf conf) {
    super(conf);
  }

  private boolean boostByLinkCount;
  private float scorePower;

  public void configure(JobConf job) {
    boostByLinkCount = job.getBoolean("indexer.boost.by.link.count", false);
    scorePower = job.getFloat("indexer.score.power", 0.5f);
  }

  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output) throws IOException {
    Inlinks inlinks = null;
    ParseData parseData = null;
    ParseText parseText = null;
    while (values.hasNext()) {
      Object value = ((ObjectWritable)values.next()).get(); // unwrap
      if (value instanceof Inlinks) {
        inlinks = (Inlinks)value;
      } else if (value instanceof ParseData) {
        parseData = (ParseData)value;
      } else if (value instanceof ParseText) {
        parseText = (ParseText)value;
      } else {
        LOG.warning("Unrecognized type: "+value.getClass());
      }
    }      

    if (parseText == null || parseData == null) {
      return;                                     // only have inlinks
    }

    Document doc = new Document();

    // add docno & segment, used to map from merged index back to segment files
    //doc.add(Field.UnIndexed("docNo", Long.toString(docNo, 16)));
    //doc.add(Field.UnIndexed("segment", segmentName));

    // add digest, used by dedup
    //doc.add(Field.UnIndexed("digest", fo.getMD5Hash().toString()));

    // 4. Apply boost to all indexed fields.
    float boost =
      IndexSegment.calculateBoost(1.0f,scorePower, boostByLinkCount,
                                  inlinks == null ? 0 : inlinks.size());
    doc.setBoost(boost);
    // store boost for use by explain and dedup
    doc.add(Field.UnIndexed("boost", Float.toString(boost)));

    try {
      doc = IndexingFilters.filter(doc, new ParseImpl(parseText, parseData),
                                   null);
    } catch (IndexingException e) {
      LOG.warning("Error indexing "+key+": "+e);
      return;
    }

    output.collect(key, new ObjectWritable(doc));
  }

  public void index(File indexDir, File segmentsDir) throws IOException {
    JobConf job = Indexer.createJob(getConf(), indexDir);
    job.setInputDir(segmentsDir);
    job.set("mapred.input.subdir", ParseData.DIR_NAME);
    JobClient.runJob(job);
  }

  public void index(File indexDir, File[] segments) throws IOException {
    JobConf job = Indexer.createJob(getConf(), indexDir);
    for (int i = 0; i < segments.length; i++) {
      job.addInputDir(new File(segments[i], ParseData.DIR_NAME));
    }
    JobClient.runJob(job);
  }

  private static JobConf createJob(NutchConf config, File indexDir) {
    JobConf job = new JobConf(config);

    job.setInputFormat(InputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(ObjectWritable.class);

    job.setMapperClass(Indexer.class);
    //job.setCombinerClass(Indexer.class);
    job.setReducerClass(Indexer.class);

    job.setOutputDir(indexDir);
    job.setOutputFormat(OutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(ObjectWritable.class);

    return job;
  }

  public static void main(String[] args) throws Exception {
    Indexer indexer = new Indexer(NutchConf.get());
    
    if (args.length < 2) {
      System.err.println("Usage: <linkdb> <segments>");
      return;
    }
    
    indexer.index(new File(args[0]), new File(args[1]));
  }



}
