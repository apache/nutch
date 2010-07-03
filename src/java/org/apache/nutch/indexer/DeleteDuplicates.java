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
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.nutch.util.TimingUtil;

/**
 * Delete duplicate documents in a set of Lucene indexes.
 * Duplicates have either the same contents (via MD5 hash) or the same URL.
 * 
 * This tool uses the following algorithm:
 * 
 * <ul>
 * <li><b>Phase 1 - remove URL duplicates:</b><br/>
 * In this phase documents with the same URL
 * are compared, and only the most recent document is retained -
 * all other URL duplicates are scheduled for deletion.</li>
 * <li><b>Phase 2 - remove content duplicates:</b><br/>
 * In this phase documents with the same content hash are compared. If
 * property "dedup.keep.highest.score" is set to true (default) then only
 * the document with the highest score is retained. If this property is set
 * to false, only the document with the shortest URL is retained - all other
 * content duplicates are scheduled for deletion.</li>
 * <li><b>Phase 3 - delete documents:</b><br/>
 * In this phase documents scheduled for deletion are marked as deleted in
 * Lucene index(es).</li>
 * </ul>
 * 
 * @author Andrzej Bialecki
 */
public class DeleteDuplicates extends Configured
  implements Tool, Mapper<WritableComparable, Writable, Text, IntWritable>, Reducer<Text, IntWritable, WritableComparable, Writable>, OutputFormat<WritableComparable, Writable> {
  private static final Log LOG = LogFactory.getLog(DeleteDuplicates.class);

//   Algorithm:
//      
//   1. map indexes -> <url, <md5, url, time, urlLen, index,doc>>
//      reduce, deleting all but most recent
//
//   2. map indexes -> <md5, <md5, url, time, urlLen, index,doc>>
//      partition by md5
//      reduce, deleting all but with highest score (or shortest url).

  public static class IndexDoc implements WritableComparable {
    private Text url = new Text();
    private int urlLen;
    private float score;
    private long time;
    private MD5Hash hash = new MD5Hash();
    private Text index = new Text();              // the segment index
    private int doc;                              // within the index
    private boolean keep = true;                  // keep or discard

    public String toString() {
      return "[url=" + url + ",score=" + score + ",time=" + time
        + ",hash=" + hash + ",index=" + index + ",doc=" + doc
        + ",keep=" + keep + "]";
    }
    
    public void write(DataOutput out) throws IOException {
      url.write(out);
      out.writeFloat(score);
      out.writeLong(time);
      hash.write(out);
      index.write(out);
      out.writeInt(doc);
      out.writeBoolean(keep);
    }

    public void readFields(DataInput in) throws IOException {
      url.readFields(in);
      urlLen = url.getLength();
      score = in.readFloat();
      time = in.readLong();
      hash.readFields(in);
      index.readFields(in);
      doc = in.readInt();
      keep = in.readBoolean();
    }

    public int compareTo(Object o) {
      IndexDoc that = (IndexDoc)o;
      if (this.keep != that.keep) {
        return this.keep ? 1 : -1; 
      } else if (!this.hash.equals(that.hash)) {       // order first by hash
        return this.hash.compareTo(that.hash);
      } else if (this.time != that.time) {      // prefer more recent docs
        return this.time > that.time ? 1 : -1 ;
      } else if (this.urlLen != that.urlLen) {  // prefer shorter urls
        return this.urlLen - that.urlLen;
      } else {
        return this.score > that.score ? 1 : -1;
      }
    }

    public boolean equals(Object o) {
      IndexDoc that = (IndexDoc)o;
      return this.keep == that.keep
        && this.hash.equals(that.hash)
        && this.time == that.time
        && this.score == that.score
        && this.urlLen == that.urlLen
        && this.index.equals(that.index) 
        && this.doc == that.doc;
    }

  }

  public static class InputFormat extends FileInputFormat<Text, IndexDoc> {
    private static final long INDEX_LENGTH = Integer.MAX_VALUE;

    /** Return each index as a split. */
    public InputSplit[] getSplits(JobConf job, int numSplits)
      throws IOException {
      FileStatus[] files = listStatus(job);
      InputSplit[] splits = new InputSplit[files.length];
      for (int i = 0; i < files.length; i++) {
        FileStatus cur = files[i];
        splits[i] = new FileSplit(cur.getPath(), 0, INDEX_LENGTH, (String[])null);
      }
      return splits;
    }

    public class DDRecordReader implements RecordReader<Text, IndexDoc> {

      private IndexReader indexReader;
      private int maxDoc = 0;
      private int doc = 0;
      private Text index;
      
      public DDRecordReader(FileSplit split, JobConf job,
          Text index) throws IOException {
        try {
          indexReader = IndexReader.open(new FsDirectory(FileSystem.get(job), split.getPath(), false, job));
          maxDoc = indexReader.maxDoc();
        } catch (IOException ioe) {
          LOG.warn("Can't open index at " + split + ", skipping. (" + ioe.getMessage() + ")");
          indexReader = null;
        }
        this.index = index;
      }

      public boolean next(Text key, IndexDoc indexDoc)
        throws IOException {
        
        // skip empty indexes
        if (indexReader == null || maxDoc <= 0)
          return false;

        // skip deleted documents
        while (doc < maxDoc && indexReader.isDeleted(doc)) doc++;
        if (doc >= maxDoc)
          return false;

        Document document = indexReader.document(doc);

        // fill in key
        key.set(document.get("url"));
        // fill in value
        indexDoc.keep = true;
        indexDoc.url.set(document.get("url"));
        indexDoc.hash.setDigest(document.get("digest"));
        indexDoc.score = Float.parseFloat(document.get("boost"));
        try {
          indexDoc.time = DateTools.stringToTime(document.get("tstamp"));
        } catch (Exception e) {
          // try to figure out the time from segment name
          try {
            String segname = document.get("segment");
            indexDoc.time = new SimpleDateFormat("yyyyMMddHHmmss").parse(segname).getTime();
            // make it unique
            indexDoc.time += doc;
          } catch (Exception e1) {
            // use current time
            indexDoc.time = System.currentTimeMillis();
          }
        }
        indexDoc.index = index;
        indexDoc.doc = doc;

        doc++;

        return true;
      }

      public long getPos() throws IOException {
        return maxDoc == 0 ? 0 : (doc*INDEX_LENGTH)/maxDoc;
      }

      public void close() throws IOException {
        if (indexReader != null) indexReader.close();
      }
      
      public Text createKey() {
        return new Text();
      }
      
      public IndexDoc createValue() {
        return new IndexDoc();
      }

      public float getProgress() throws IOException {
        return maxDoc == 0 ? 0.0f : (float)doc / (float)maxDoc;
      }
    }
    
    /** Return each index as a split. */
    public RecordReader<Text, IndexDoc> getRecordReader(InputSplit split,
                                        JobConf job,
                                        Reporter reporter) throws IOException {
      FileSplit fsplit = (FileSplit)split;
      Text index = new Text(fsplit.getPath().toString());
      reporter.setStatus(index.toString());
      return new DDRecordReader(fsplit, job, index);
    }
  }
  
  public static class HashPartitioner implements Partitioner<MD5Hash, Writable> {
    public void configure(JobConf job) {}
    public void close() {}
    public int getPartition(MD5Hash key, Writable value,
                            int numReduceTasks) {
      int hashCode = key.hashCode();
      return (hashCode & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  public static class UrlsReducer implements Reducer<Text, IndexDoc, MD5Hash, IndexDoc> {
    
    public void configure(JobConf job) {}
    
    public void close() {}
    
    private IndexDoc latest = new IndexDoc();
    
    public void reduce(Text key, Iterator<IndexDoc> values,
        OutputCollector<MD5Hash, IndexDoc> output, Reporter reporter) throws IOException {
      WritableUtils.cloneInto(latest, values.next());
      while (values.hasNext()) {
        IndexDoc value = values.next();
        if (value.time > latest.time) {
          // discard current and use more recent
          latest.keep = false;
          LOG.debug("-discard " + latest + ", keep " + value);
          output.collect(latest.hash, latest);
          WritableUtils.cloneInto(latest, value);
        } else {
          // discard
          value.keep = false;
          LOG.debug("-discard " + value + ", keep " + latest);
          output.collect(value.hash, value);
        }
        
      }
      // keep the latest
      latest.keep = true;
      output.collect(latest.hash, latest);
      
    }
  }
  
  public static class HashReducer implements Reducer<MD5Hash, IndexDoc, Text, IndexDoc> {
    boolean byScore;
    
    public void configure(JobConf job) {
      byScore = job.getBoolean("dedup.keep.highest.score", true);
    }
    
    public void close() {}
    
    private IndexDoc highest = new IndexDoc();
    
    public void reduce(MD5Hash key, Iterator<IndexDoc> values,
                       OutputCollector<Text, IndexDoc> output, Reporter reporter)
      throws IOException {
      boolean highestSet = false;
      while (values.hasNext()) {
        IndexDoc value = values.next();
        // skip already deleted
        if (!value.keep) {
          LOG.debug("-discard " + value + " (already marked)");
          output.collect(value.url, value);
          continue;
        }
        if (!highestSet) {
          WritableUtils.cloneInto(highest, value);
          highestSet = true;
          continue;
        }
        IndexDoc toDelete = null, toKeep = null;
        boolean metric = byScore ? (value.score > highest.score) : 
                                   (value.urlLen < highest.urlLen);
        if (metric) {
          toDelete = highest;
          toKeep = value;
        } else {
          toDelete = value;
          toKeep = highest;
        }
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("-discard " + toDelete + ", keep " + toKeep);
        }
        
        toDelete.keep = false;
        output.collect(toDelete.url, toDelete);
        WritableUtils.cloneInto(highest, toKeep);
      }    
      LOG.debug("-keep " + highest);
      // no need to add this - in phase 2 we only process docs to delete them
      // highest.keep = true;
      // output.collect(key, highest);
    }
  }
    
  private FileSystem fs;

  public void configure(JobConf job) {
    setConf(job);
  }
  
  public void setConf(Configuration conf) {
    super.setConf(conf);
    try {
      if(conf != null) fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {}

  /** Map [*,IndexDoc] pairs to [index,doc] pairs. */
  public void map(WritableComparable key, Writable value,
                  OutputCollector<Text, IntWritable> output, Reporter reporter)
    throws IOException {
    IndexDoc indexDoc = (IndexDoc)value;
    // don't delete these
    if (indexDoc.keep) return;
    // delete all others
    output.collect(indexDoc.index, new IntWritable(indexDoc.doc));
  }

  /** Delete docs named in values from index named in key. */
  public void reduce(Text key, Iterator<IntWritable> values,
                     OutputCollector<WritableComparable, Writable> output, Reporter reporter)
    throws IOException {
    Path index = new Path(key.toString());
    IndexReader reader = IndexReader.open(new FsDirectory(fs, index, false, getConf()), false);
    try {
      while (values.hasNext()) {
        IntWritable value = values.next();
        LOG.debug("-delete " + index + " doc=" + value);
        reader.deleteDocument(value.get());
      }
    } finally {
      reader.close();
    }
  }

  /** Write nothing. */
  public RecordWriter<WritableComparable, Writable> getRecordWriter(final FileSystem fs,
                                      final JobConf job,
                                      final String name,
                                      final Progressable progress) throws IOException {
    return new RecordWriter<WritableComparable, Writable>() {                   
        public void write(WritableComparable key, Writable value)
          throws IOException {
          throw new UnsupportedOperationException();
        }        
        public void close(Reporter reporter) throws IOException {}
      };
  }

  public DeleteDuplicates() {
    
  }
  
  public DeleteDuplicates(Configuration conf) {
    setConf(conf);
  }
  
  public void checkOutputSpecs(FileSystem fs, JobConf job) {}

  public void dedup(Path[] indexDirs)
    throws IOException {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("Dedup: starting at " + sdf.format(start));

    Path outDir1 =
      new Path("dedup-urls-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(getConf());

    for (int i = 0; i < indexDirs.length; i++) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Dedup: adding indexes in: " + indexDirs[i]);
      }
      FileInputFormat.addInputPath(job, indexDirs[i]);
    }
    job.setJobName("dedup 1: urls by time");

    job.setInputFormat(InputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IndexDoc.class);

    job.setReducerClass(UrlsReducer.class);
    FileOutputFormat.setOutputPath(job, outDir1);

    job.setOutputKeyClass(MD5Hash.class);
    job.setOutputValueClass(IndexDoc.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);

    JobClient.runJob(job);

    Path outDir2 =
      new Path("dedup-hash-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
    job = new NutchJob(getConf());
    job.setJobName("dedup 2: content by hash");

    FileInputFormat.addInputPath(job, outDir1);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(MD5Hash.class);
    job.setMapOutputValueClass(IndexDoc.class);
    job.setPartitionerClass(HashPartitioner.class);
    job.setSpeculativeExecution(false);
    
    job.setReducerClass(HashReducer.class);
    FileOutputFormat.setOutputPath(job, outDir2);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IndexDoc.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);

    JobClient.runJob(job);

    // remove outDir1 - no longer needed
    fs.delete(outDir1, true);
    
    job = new NutchJob(getConf());
    job.setJobName("dedup 3: delete from index(es)");

    FileInputFormat.addInputPath(job, outDir2);
    job.setInputFormat(SequenceFileInputFormat.class);
    //job.setInputKeyClass(Text.class);
    //job.setInputValueClass(IndexDoc.class);

    job.setInt("io.file.buffer.size", 4096);
    job.setMapperClass(DeleteDuplicates.class);
    job.setReducerClass(DeleteDuplicates.class);

    job.setOutputFormat(DeleteDuplicates.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    JobClient.runJob(job);

    fs.delete(outDir2, true);

    long end = System.currentTimeMillis();
    LOG.info("Dedup: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new DeleteDuplicates(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    
    if (args.length < 1) {
      System.err.println("Usage: DeleteDuplicates <indexes> ...");
      return -1;
    }
    
    Path[] indexes = new Path[args.length];
    for (int i = 0; i < args.length; i++) {
      indexes[i] = new Path(args[i]);
    }
    try {
      dedup(indexes);
      return 0;
    } catch (Exception e) {
      LOG.fatal("DeleteDuplicates: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

}
