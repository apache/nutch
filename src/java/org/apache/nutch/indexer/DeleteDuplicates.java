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

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.mapred.*;

import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.document.Document;

/******************************************************************
 * Deletes duplicate documents in a set of Lucene indexes.
 * Duplicates have either the same contents (via MD5 hash) or the same URL.
 ******************************************************************/
public class DeleteDuplicates extends Configured
  implements Mapper, Reducer, OutputFormat {
  private static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.indexer.DeleteDuplicates");

//   Algorithm:
//      
//   1. map indexes -> <<md5, score, urlLen>, <index,doc>>
//      partition by md5
//      reduce, deleting all but largest score w/ shortest url
//
//   2. map indexes -> <<url, fetchdate>, <index,doc>>
//      partition by url
//      reduce, deleting all but most recent.
//
//   Part 2 is not yet implemented, but the Indexer currently only indexes one
//   URL per page, so this is not a critical problem.

  public static class IndexDoc implements WritableComparable {
    private UTF8 index;                           // the segment index
    private int doc;                              // within the index

    public void write(DataOutput out) throws IOException {
      index.write(out);
      out.writeInt(doc);
    }

    public void readFields(DataInput in) throws IOException {
      if (index == null) {
        index = new UTF8();
      }
      index.readFields(in);
      this.doc = in.readInt();
    }

    public int compareTo(Object o) {
      IndexDoc that = (IndexDoc)o;
      int indexCompare = this.index.compareTo(that.index);
      if (indexCompare != 0) {                    // prefer later indexes
        return indexCompare;
      } else {
        return this.doc - that.doc;               // prefer later docs
      }
    }

    public boolean equals(Object o) {
      IndexDoc that = (IndexDoc)o;
      return this.index.equals(that.index) && this.doc == that.doc;
    }

  }

  public static class HashScore implements WritableComparable {
    private MD5Hash hash;
    private float score;
    private int urlLen;

    public void write(DataOutput out) throws IOException {
      hash.write(out);
      out.writeFloat(score);
      out.writeInt(urlLen);
    }

    public void readFields(DataInput in) throws IOException {
      if (hash == null) {
        hash = new MD5Hash();
      }
      hash.readFields(in);
      score = in.readFloat();
      urlLen = in.readInt();
    }

    public int compareTo(Object o) {
      HashScore that = (HashScore)o;
      if (!this.hash.equals(that.hash)) {         // order first by hash
        return this.hash.compareTo(that.hash);
      } else if (this.score != that.score) {      // prefer larger scores
        return this.score < that.score ? 1 : -1 ;
      } else {                                    // prefer shorter urls
        return this.urlLen - that.urlLen;
      }
    }

    public boolean equals(Object o) {
      HashScore that = (HashScore)o;
      return this.hash.equals(that.hash)
        && this.score == that.score
        && this.urlLen == that.urlLen;
    }
  }

  public static class InputFormat extends InputFormatBase {
    private static final long INDEX_LENGTH = Integer.MAX_VALUE;

    /** Return each index as a split. */
    public FileSplit[] getSplits(FileSystem fs, JobConf job,
                                 int numSplits)
      throws IOException {
      File[] files = listFiles(fs, job);
      FileSplit[] splits = new FileSplit[files.length];
      for (int i = 0; i < files.length; i++) {
        splits[i] = new FileSplit(files[i], 0, INDEX_LENGTH);
      }
      return splits;
    }

    /** Return each index as a split. */
    public RecordReader getRecordReader(final FileSystem fs,
                                        final FileSplit split,
                                        final JobConf job,
                                        Reporter reporter) throws IOException {
      final UTF8 index = new UTF8(split.getFile().toString());
      reporter.setStatus(index.toString());
      return new RecordReader() {

          private IndexReader indexReader =
            IndexReader.open(new FsDirectory(fs, split.getFile(), false, job));

          { indexReader.undeleteAll(); }

          private final int maxDoc = indexReader.maxDoc();
          private int doc;

          public boolean next(Writable key, Writable value)
            throws IOException {

            if (doc >= maxDoc)
              return false;

            Document document = indexReader.document(doc);

            // fill in key
            if (key instanceof UTF8) {
              ((UTF8)key).set(document.get("url"));
            } else {
              HashScore hashScore = (HashScore)key;
              if (hashScore.hash == null) {
                hashScore.hash = new MD5Hash();
              }
              hashScore.hash.setDigest(document.get("digest"));
              hashScore.score = Float.parseFloat(document.get("boost"));
              hashScore.urlLen = document.get("url").length();
            }

            // fill in value
            IndexDoc indexDoc = (IndexDoc)value;
            if (indexDoc.index == null) {
              indexDoc.index = new UTF8();
            }
            indexDoc.index.set(index);
            indexDoc.doc = doc;

            doc++;

            return true;
          }

          public long getPos() throws IOException {
            return maxDoc==0 ? 0 : (doc*INDEX_LENGTH)/maxDoc;
          }

          public void close() throws IOException {
            indexReader.close();
          }
        };
    }
  }

  public static class HashPartitioner implements Partitioner {
    public void configure(JobConf job) {}
    public void close() {}
    public int getPartition(WritableComparable key, Writable value,
                            int numReduceTasks) {
      int hashCode = ((HashScore)key).hash.hashCode();
      return (hashCode & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  public static class HashReducer implements Reducer {
    private MD5Hash prevHash = new MD5Hash();
    public void configure(JobConf job) {}
    public void close() {}
    public void reduce(WritableComparable key, Iterator values,
                       OutputCollector output, Reporter reporter)
      throws IOException {
      MD5Hash hash = ((HashScore)key).hash;
      while (values.hasNext()) {
        Writable value = (Writable)values.next();
        if (hash.equals(prevHash)) {                // collect all but first
          output.collect(key, value);
        } else {
          prevHash.set(hash);
        }
      }
    }
  }
    
  private FileSystem fs;
  private int ioFileBufferSize;

  public DeleteDuplicates() { super(null); }

  public DeleteDuplicates(Configuration conf) { super(conf); }

  public void configure(JobConf job) {
    setConf(job);
    try {
      fs = FileSystem.get(job);
      this.ioFileBufferSize = job.getInt("io.file.buffer.size", 4096);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {}

  /** Map [*,IndexDoc] pairs to [index,doc] pairs. */
  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter)
    throws IOException {
    IndexDoc indexDoc = (IndexDoc)value;
    output.collect(indexDoc.index, new IntWritable(indexDoc.doc));
  }

  /** Delete docs named in values from index named in key. */
  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter)
    throws IOException {
    File index = new File(key.toString());
    IndexReader reader = IndexReader.open(new FsDirectory(fs, index, false, getConf()));
    try {
      while (values.hasNext()) {
        reader.delete(((IntWritable)values.next()).get());
      }
    } finally {
      reader.close();
    }
  }

  /** Write nothing. */
  public RecordWriter getRecordWriter(final FileSystem fs,
                                      final JobConf job,
                                      final String name) throws IOException {
    return new RecordWriter() {                   
        public void write(WritableComparable key, Writable value)
          throws IOException {
          throw new UnsupportedOperationException();
        }        
        public void close(Reporter reporter) throws IOException {}
      };
  }

  public void dedup(File[] indexDirs)
    throws IOException {

    LOG.info("Dedup: starting");

    File hashDir =
      new File("dedup-hash-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(getConf());

    for (int i = 0; i < indexDirs.length; i++) {
      LOG.info("Dedup: adding indexes in: " + indexDirs[i]);
      job.addInputDir(indexDirs[i]);
    }

    job.setInputKeyClass(HashScore.class);
    job.setInputValueClass(IndexDoc.class);
    job.setInputFormat(InputFormat.class);

    job.setPartitionerClass(HashPartitioner.class);
    job.setReducerClass(HashReducer.class);

    job.setOutputDir(hashDir);

    job.setOutputKeyClass(HashScore.class);
    job.setOutputValueClass(IndexDoc.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);

    JobClient.runJob(job);

    job = new NutchJob(getConf());

    job.addInputDir(hashDir);

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(HashScore.class);
    job.setInputValueClass(IndexDoc.class);

    job.setInt("io.file.buffer.size", 4096);
    job.setMapperClass(DeleteDuplicates.class);
    job.setReducerClass(DeleteDuplicates.class);

    job.setOutputFormat(DeleteDuplicates.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(IntWritable.class);

    JobClient.runJob(job);

    new JobClient(getConf()).getFs().delete(hashDir);

    LOG.info("Dedup: done");
  }

  public static boolean doMain(String[] args) throws Exception {
    DeleteDuplicates dedup = new DeleteDuplicates(NutchConfiguration.create());
    
    if (args.length < 1) {
      System.err.println("Usage: <indexes> ...");
      return false;
    }
    
    File[] indexes = new File[args.length];
    for (int i = 0; i < args.length; i++) {
      indexes[i] = new File(args[i]);
    }

    dedup.dedup(indexes);

    return true;
  }

  /**
   * main() wrapper that returns proper exit status
   */
  public static void main(String[] args) {
    Runtime rt = Runtime.getRuntime();
    try {
      boolean status = doMain(args);
      rt.exit(status ? 0 : 1);
    }
    catch (Exception e) {
      LOG.log(Level.SEVERE, "error, caught Exception in main()", e);
      rt.exit(1);
    }
  }
}
