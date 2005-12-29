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

package org.apache.nutch.indexer;

import java.io.*;
import java.util.*;
import java.util.logging.*;

import org.apache.nutch.io.*;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.fs.*;
import org.apache.nutch.net.*;
import org.apache.nutch.util.*;
import org.apache.nutch.mapred.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.analysis.*;

import org.apache.nutch.indexer.*;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.crawl.LinkDb;

import org.apache.lucene.index.*;
import org.apache.lucene.document.*;

/** Create indexes for segments. */
public class Indexer extends NutchConfigured implements Reducer {
  
  public static final String DONE_NAME = "index.done";

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.crawl.Indexer");

  /** Wraps inputs in an {@link ObjectWritable}, to permit merging different
   * types in reduce. */
  public static class InputFormat extends SequenceFileInputFormat {
    public RecordReader getRecordReader(NutchFileSystem fs, FileSplit split,
                                        JobConf job, Reporter reporter)
      throws IOException {

      reporter.setStatus(split.toString());
      
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
      final File temp =
        job.getLocalFile("index","_"+Integer.toString(new Random().nextInt()));

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
      //writer.infoStream = LogFormatter.getLogStream(LOG, Level.FINE);
      writer.setUseCompoundFile(false);
      writer.setSimilarity(new NutchSimilarity());

      return new RecordWriter() {
          boolean closed;

          public void write(WritableComparable key, Writable value)
            throws IOException {                  // unwrap & index doc
            writer.addDocument((Document)((ObjectWritable)value).get());
          }
          
          public void close(final Reporter reporter) throws IOException {
            // spawn a thread to give progress heartbeats
            Thread prog = new Thread() {
                public void run() {
                  while (!closed) {
                    try {
                      reporter.setStatus("closing");
                      Thread.sleep(1000);
                    } catch (InterruptedException e) { continue; }
                      catch (Throwable e) { return; }
                  }
                }
              };

            try {
              prog.start();
              LOG.info("Optimizing index.");        // optimize & close index
              writer.optimize();
              writer.close();
              fs.completeLocalOutput(perm, temp);   // copy to ndfs
              fs.createNewFile(new File(perm, DONE_NAME));
            } finally {
              closed = true;
            }
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

  private float scorePower;

  public void configure(JobConf job) {
    scorePower = job.getFloat("indexer.score.power", 0.5f);
  }

  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter)
    throws IOException {
    Inlinks inlinks = null;
    CrawlDatum dbDatum = null;
    CrawlDatum fetchDatum = null;
    ParseData parseData = null;
    ParseText parseText = null;
    while (values.hasNext()) {
      Object value = ((ObjectWritable)values.next()).get(); // unwrap
      if (value instanceof Inlinks) {
        inlinks = (Inlinks)value;
      } else if (value instanceof CrawlDatum) {
        CrawlDatum datum = (CrawlDatum)value;
        switch (datum.getStatus()) {
        case CrawlDatum.STATUS_DB_UNFETCHED:
        case CrawlDatum.STATUS_DB_FETCHED:
        case CrawlDatum.STATUS_DB_GONE:
          dbDatum = datum;
          break;
        case CrawlDatum.STATUS_FETCH_SUCCESS:
        case CrawlDatum.STATUS_FETCH_RETRY:
        case CrawlDatum.STATUS_FETCH_GONE:
          fetchDatum = datum;
          break;
        default:
          throw new RuntimeException("Unexpected status: "+datum.getStatus());
        }
      } else if (value instanceof ParseData) {
        parseData = (ParseData)value;
      } else if (value instanceof ParseText) {
        parseText = (ParseText)value;
      } else {
        LOG.warning("Unrecognized type: "+value.getClass());
      }
    }      

    if (fetchDatum == null || dbDatum == null
        || parseText == null || parseData == null) {
      return;                                     // only have inlinks
    }

    Document doc = new Document();
    ContentProperties meta = parseData.getMetadata();
    String[] anchors = inlinks!=null ? inlinks.getAnchors() : new String[0];

    // add segment, used to map from merged index back to segment files
    doc.add(Field.UnIndexed("segment",
                            meta.getProperty(Fetcher.SEGMENT_NAME_KEY)));

    // add digest, used by dedup
    doc.add(Field.UnIndexed("digest", meta.getProperty(Fetcher.DIGEST_KEY)));

    // boost is opic
    float boost = (float)Math.pow(dbDatum.getScore(), scorePower);
    // apply boost to all indexed fields.
    doc.setBoost(boost);
    // store boost for use by explain and dedup
    doc.add(Field.UnIndexed("boost", Float.toString(boost)));

//     LOG.info("Url: "+key.toString());
//     LOG.info("Title: "+parseData.getTitle());
//     LOG.info(crawlDatum.toString());
//     if (inlinks != null) {
//       LOG.info(inlinks.toString());
//     }

    try {
      // run indexing filters
      doc = IndexingFilters.filter(doc,new ParseImpl(parseText, parseData), (UTF8)key, fetchDatum, inlinks);
    } catch (IndexingException e) {
      LOG.warning("Error indexing "+key+": "+e);
      return;
    }

    output.collect(key, new ObjectWritable(doc));
  }

  public void index(File indexDir, File crawlDb, File linkDb, File[] segments)
    throws IOException {

    LOG.info("Indexer: starting");
    LOG.info("Indexer: linkdb: " + linkDb);

    JobConf job = new JobConf(getConf());

    for (int i = 0; i < segments.length; i++) {
      LOG.info("Indexer: adding segment: " + segments[i]);
      job.addInputDir(new File(segments[i], CrawlDatum.FETCH_DIR_NAME));
      job.addInputDir(new File(segments[i], ParseData.DIR_NAME));
      job.addInputDir(new File(segments[i], ParseText.DIR_NAME));
    }

    job.addInputDir(new File(crawlDb, CrawlDatum.DB_DIR_NAME));
    job.addInputDir(new File(linkDb, LinkDb.CURRENT_NAME));

    job.setInputFormat(InputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(ObjectWritable.class);

    //job.setCombinerClass(Indexer.class);
    job.setReducerClass(Indexer.class);

    job.setOutputDir(indexDir);
    job.setOutputFormat(OutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(ObjectWritable.class);

    JobClient.runJob(job);
    LOG.info("Indexer: done");
  }

  public static void main(String[] args) throws Exception {
    Indexer indexer = new Indexer(NutchConf.get());
    
    if (args.length < 4) {
      System.err.println("Usage: <index> <crawldb> <linkdb> <segment> ...");
      return;
    }
    
    File[] segments = new File[args.length-3];
    for (int i = 3; i < args.length; i++) {
      segments[i-3] = new File(args[i]);
    }

    indexer.index(new File(args[0]), new File(args[1]), new File(args[2]),
                  segments);
  }

}
