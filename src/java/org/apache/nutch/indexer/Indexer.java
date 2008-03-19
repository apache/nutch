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

package org.apache.nutch.indexer;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.analysis.*;

import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.crawl.LinkDb;
import org.apache.nutch.crawl.NutchWritable;

import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;

/** Create indexes for segments. */
public class Indexer extends Configured implements Tool, Reducer<Text, NutchWritable, Text, Writable>, Mapper<Text, Writable, Text, NutchWritable> {
  
  public static final String DONE_NAME = "index.done";

  public static final Log LOG = LogFactory.getLog(Indexer.class);
  
  /** A utility class used to pass a lucene document from Indexer.reduce 
   * to Indexer.OutputFormat.
   * Note: Despite its name, it can't properly wrap a lucene document - it
   * doesn't know how to serialize/deserialize a lucene document.
   */
  private static class LuceneDocumentWrapper implements Writable {
    private Document doc;
	  
    public LuceneDocumentWrapper(Document doc) {
      this.doc = doc;
    }
    
    public Document get() {
      return doc;
    }

    public void readFields(DataInput in) throws IOException { 
      // intentionally left blank
    }
		
    public void write(DataOutput out) throws IOException {
      // intentionally left blank
    }
	  
  }

  /** Unwrap Lucene Documents created by reduce and add them to an index. */
  public static class OutputFormat
    extends org.apache.hadoop.mapred.OutputFormatBase<WritableComparable, LuceneDocumentWrapper> {
    public RecordWriter<WritableComparable, LuceneDocumentWrapper> getRecordWriter(final FileSystem fs, JobConf job,
                                        String name, final Progressable progress) throws IOException {
      final Path perm = new Path(job.getOutputPath(), name);
      final Path temp =
        job.getLocalPath("index/_"+Integer.toString(new Random().nextInt()));

      fs.delete(perm);                            // delete old, if any

      final AnalyzerFactory factory = new AnalyzerFactory(job);
      final IndexWriter writer =                  // build locally first
        new IndexWriter(fs.startLocalOutput(perm, temp).toString(),
                        new NutchDocumentAnalyzer(job), true);

      writer.setMergeFactor(job.getInt("indexer.mergeFactor", 10));
      writer.setMaxBufferedDocs(job.getInt("indexer.minMergeDocs", 100));
      writer.setMaxMergeDocs(job.getInt("indexer.maxMergeDocs", Integer.MAX_VALUE));
      writer.setTermIndexInterval
        (job.getInt("indexer.termIndexInterval", 128));
      writer.setMaxFieldLength(job.getInt("indexer.max.tokens", 10000));
      writer.setInfoStream(LogUtil.getInfoStream(LOG));
      writer.setUseCompoundFile(false);
      writer.setSimilarity(new NutchSimilarity());

      return new RecordWriter<WritableComparable, LuceneDocumentWrapper>() {
          boolean closed;

          public void write(WritableComparable key, LuceneDocumentWrapper value)
            throws IOException {                  // unwrap & index doc
            Document doc = value.get();
            NutchAnalyzer analyzer = factory.get(doc.get("lang"));
            if (LOG.isInfoEnabled()) {
              LOG.info(" Indexing [" + doc.getField("url").stringValue() + "]" +
                       " with analyzer " + analyzer +
                       " (" + doc.get("lang") + ")");
            }
            writer.addDocument(doc, analyzer);
            progress.progress();
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
              if (LOG.isInfoEnabled()) { LOG.info("Optimizing index."); }
              // optimize & close index
              writer.optimize();
              writer.close();
              fs.completeLocalOutput(perm, temp);   // copy to dfs
              fs.createNewFile(new Path(perm, DONE_NAME));
            } finally {
              closed = true;
            }
          }
        };
    }
  }

  private IndexingFilters filters;
  private ScoringFilters scfilters;

  public Indexer() {
    
  }
  
  public Indexer(Configuration conf) {
    setConf(conf);
  }
  
  public void configure(JobConf job) {
    setConf(job);
    this.filters = new IndexingFilters(getConf());
    this.scfilters = new ScoringFilters(getConf());
  }

  public void close() {}

  public void reduce(Text key, Iterator<NutchWritable> values,
                     OutputCollector<Text, Writable> output, Reporter reporter)
    throws IOException {
    Inlinks inlinks = null;
    CrawlDatum dbDatum = null;
    CrawlDatum fetchDatum = null;
    ParseData parseData = null;
    ParseText parseText = null;
    while (values.hasNext()) {
      Writable value = values.next().get(); // unwrap
      if (value instanceof Inlinks) {
        inlinks = (Inlinks)value;
      } else if (value instanceof CrawlDatum) {
        CrawlDatum datum = (CrawlDatum)value;
        if (CrawlDatum.hasDbStatus(datum))
          dbDatum = datum;
        else if (CrawlDatum.hasFetchStatus(datum)) {
          // don't index unmodified (empty) pages
          if (datum.getStatus() != CrawlDatum.STATUS_FETCH_NOTMODIFIED)
            fetchDatum = datum;
        } else if (CrawlDatum.STATUS_LINKED == datum.getStatus() ||
                   CrawlDatum.STATUS_SIGNATURE == datum.getStatus()) {
          continue;
        } else {
          throw new RuntimeException("Unexpected status: "+datum.getStatus());
        }
      } else if (value instanceof ParseData) {
        parseData = (ParseData)value;
      } else if (value instanceof ParseText) {
        parseText = (ParseText)value;
      } else if (LOG.isWarnEnabled()) {
        LOG.warn("Unrecognized type: "+value.getClass());
      }
    }      

    if (fetchDatum == null || dbDatum == null
        || parseText == null || parseData == null) {
      return;                                     // only have inlinks
    }
    
    if (!parseData.getStatus().isSuccess() || 
        fetchDatum.getStatus() != CrawlDatum.STATUS_FETCH_SUCCESS) {
      return;
    }

    Document doc = new Document();
    Metadata metadata = parseData.getContentMeta();

    // add segment, used to map from merged index back to segment files
    doc.add(new Field("segment", metadata.get(Nutch.SEGMENT_NAME_KEY),
            Field.Store.YES, Field.Index.NO));

    // add digest, used by dedup
    doc.add(new Field("digest", metadata.get(Nutch.SIGNATURE_KEY),
            Field.Store.YES, Field.Index.NO));

//     if (LOG.isInfoEnabled()) {
//       LOG.info("Url: "+key.toString());
//       LOG.info("Title: "+parseData.getTitle());
//       LOG.info(crawlDatum.toString());
//       if (inlinks != null) {
//         LOG.info(inlinks.toString());
//       }
//     }

    Parse parse = new ParseImpl(parseText, parseData);
    try {
      // extract information from dbDatum and pass it to
      // fetchDatum so that indexing filters can use it
      Text url = (Text) dbDatum.getMetaData().get(Nutch.WRITABLE_REPR_URL_KEY);
      if (url != null) {
        fetchDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY, url);
      }
      // run indexing filters
      doc = this.filters.filter(doc, parse, key, fetchDatum, inlinks);
    } catch (IndexingException e) {
      if (LOG.isWarnEnabled()) { LOG.warn("Error indexing "+key+": "+e); }
      return;
    }

    // skip documents discarded by indexing filters
    if (doc == null) return;
    
    float boost = 1.0f;
    // run scoring filters
    try {
      boost = this.scfilters.indexerScore((Text)key, doc, dbDatum,
              fetchDatum, parse, inlinks, boost);
    } catch (ScoringFilterException e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Error calculating score " + key + ": " + e);
      }
      return;
    }
    // apply boost to all indexed fields.
    doc.setBoost(boost);
    // store boost for use by explain and dedup
    doc.add(new Field("boost", Float.toString(boost),
            Field.Store.YES, Field.Index.NO));

    output.collect(key, new LuceneDocumentWrapper(doc));
  }

  public void index(Path indexDir, Path crawlDb, Path linkDb, Path[] segments)
    throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("Indexer: starting");
      LOG.info("Indexer: linkdb: " + linkDb);
    }

    JobConf job = new NutchJob(getConf());
    job.setJobName("index " + indexDir);

    for (int i = 0; i < segments.length; i++) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Indexer: adding segment: " + segments[i]);
      }
      job.addInputPath(new Path(segments[i], CrawlDatum.FETCH_DIR_NAME));
      job.addInputPath(new Path(segments[i], CrawlDatum.PARSE_DIR_NAME));
      job.addInputPath(new Path(segments[i], ParseData.DIR_NAME));
      job.addInputPath(new Path(segments[i], ParseText.DIR_NAME));
    }

    job.addInputPath(new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.addInputPath(new Path(linkDb, LinkDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(Indexer.class);
    job.setReducerClass(Indexer.class);

    job.setOutputPath(indexDir);
    job.setOutputFormat(OutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NutchWritable.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) { LOG.info("Indexer: done"); }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new Indexer(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    
    if (args.length < 4) {
      System.err.println("Usage: <index> <crawldb> <linkdb> <segment> ...");
      return -1;
    }
    
    Path[] segments = new Path[args.length-3];
    for (int i = 3; i < args.length; i++) {
      segments[i-3] = new Path(args[i]);
    }

    try {
      index(new Path(args[0]), new Path(args[1]), new Path(args[2]),
                  segments);
      return 0;
    } catch (Exception e) {
      LOG.fatal("Indexer: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public void map(Text key, Writable value,
      OutputCollector<Text, NutchWritable> output, Reporter reporter) throws IOException {
    output.collect(key, new NutchWritable(value));
  }

}
