/*
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
package org.apache.nutch.indexer.solr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

/** 
 * Utility class for deleting duplicate documents from a solr index.
 *
 * The algorithm goes like follows:
 * 
 * Preparation:
 * <ol>
 * <li>Query the solr server for the number of documents (say, N)</li>
 * <li>Partition N among M map tasks. For example, if we have two map tasks
 * the first map task will deal with solr documents from 0 - (N / 2 - 1) and
 * the second will deal with documents from (N / 2) to (N - 1).</li>
 * </ol>
 * 
 * MapReduce:
 * <ul>
 * <li>Map: Identity map where keys are digests and values are {@link SolrRecord}
 * instances(which contain id, boost and timestamp)</li>
 * <li>Reduce: After map, {@link SolrRecord}s with the same digest will be
 * grouped together. Now, of these documents with the same digests, delete
 * all of them except the one with the highest score (boost field). If two
 * (or more) documents have the same score, then the document with the latest
 * timestamp is kept. Again, every other is deleted from solr index.
 * </li>
 * </ul>
 * 
 * Note that we assume that two documents in
 * a solr index will never have the same URL. So this class only deals with
 * documents with <b>different</b> URLs but the same digest. 
 */
public class SolrDeleteDuplicates
extends Reducer<Text, SolrDeleteDuplicates.SolrRecord, Text, SolrDeleteDuplicates.SolrRecord>
implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(SolrDeleteDuplicates.class);

  private static final String SOLR_GET_ALL_QUERY = SolrConstants.ID_FIELD + ":[* TO *]";

  private static final int NUM_MAX_DELETE_REQUEST = 1000;

  public static class SolrRecord implements Writable {

    private float boost;
    private long tstamp;
    private String id;

    public SolrRecord() { }

    public SolrRecord(String id, float boost, long tstamp) {
      this.id = id;
      this.boost = boost;
      this.tstamp = tstamp;
    }

    public String getId() {
      return id;
    }

    public float getBoost() {
      return boost;
    }

    public long getTstamp() {
      return tstamp;
    }

    public void readSolrDocument(SolrDocument doc) {
      id = (String)doc.getFieldValue(SolrConstants.ID_FIELD);
      boost = (Float)doc.getFieldValue(SolrConstants.BOOST_FIELD);

      Date buffer = (Date)doc.getFieldValue(SolrConstants.TIMESTAMP_FIELD);
      tstamp = buffer.getTime();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      id = Text.readString(in);
      boost = in.readFloat();
      tstamp = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, id);
      out.writeFloat(boost);
      out.writeLong(tstamp);
    } 
  }

  public static class SolrInputSplit extends InputSplit implements Writable {

    private int docBegin;
    private int numDocs;

    public SolrInputSplit() { }

    public SolrInputSplit(int docBegin, int numDocs) {
      this.docBegin = docBegin;
      this.numDocs = numDocs;
    }

    public int getDocBegin() {
      return docBegin;
    }

    @Override
    public long getLength() throws IOException {
      return numDocs;
    }

    @Override
    public String[] getLocations() throws IOException {
      return new String[] {} ;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      docBegin = in.readInt();
      numDocs = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(docBegin);
      out.writeInt(numDocs);
    } 
  }
  
  public static class SolrRecordReader extends RecordReader<Text, SolrRecord> {

    private int currentDoc = 0;
    private int numDocs;
    private Text text;
    private SolrRecord record;
    private SolrDocumentList solrDocs;
    
    public SolrRecordReader(SolrDocumentList solrDocs, int numDocs) {
      this.solrDocs = solrDocs;
      this.numDocs = numDocs;
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      text = new Text();
      record = new SolrRecord();   
    }

    @Override
    public void close() throws IOException { }

    @Override
    public float getProgress() throws IOException {
      return currentDoc / (float) numDocs;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      return text;
    }

    @Override
    public SolrRecord getCurrentValue() throws IOException,
        InterruptedException {
      return record;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (currentDoc >= numDocs) {
        return false;
      }

      SolrDocument doc = solrDocs.get(currentDoc);
      String digest = (String) doc.getFieldValue(SolrConstants.DIGEST_FIELD);
      text.set(digest);
      record.readSolrDocument(doc);

      currentDoc++;
      return true;
    }
   
  };

  public static class SolrInputFormat extends InputFormat<Text, SolrRecord> {
    
    @Override
    public List<InputSplit> getSplits(JobContext context)
    throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      int numSplits = context.getNumReduceTasks();
      SolrServer solr = SolrUtils.getCommonsHttpSolrServer(conf);

      final SolrQuery solrQuery = new SolrQuery(SOLR_GET_ALL_QUERY);
      solrQuery.setFields(SolrConstants.ID_FIELD);
      solrQuery.setRows(1);

      QueryResponse response;
      try {
        response = solr.query(solrQuery);
      } catch (final SolrServerException e) {
        throw new IOException(e);
      }

      int numResults = (int)response.getResults().getNumFound();
      int numDocsPerSplit = (numResults / numSplits); 
      int currentDoc = 0;
      List<InputSplit> splits = new ArrayList<InputSplit>();
      for (int i = 0; i < numSplits - 1; i++) {
        splits.add(new SolrInputSplit(currentDoc, numDocsPerSplit));
        currentDoc += numDocsPerSplit;
      }
      splits.add(new SolrInputSplit(currentDoc, numResults - currentDoc));

      return splits;
    }

    @Override
    public RecordReader<Text, SolrRecord> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      SolrServer solr = SolrUtils.getCommonsHttpSolrServer(conf);
      SolrInputSplit solrSplit = (SolrInputSplit) split;
      final int numDocs = (int) solrSplit.getLength();
      
      SolrQuery solrQuery = new SolrQuery(SOLR_GET_ALL_QUERY);
      solrQuery.setFields(SolrConstants.ID_FIELD, SolrConstants.BOOST_FIELD,
                          SolrConstants.TIMESTAMP_FIELD,
                          SolrConstants.DIGEST_FIELD);
      solrQuery.setStart(solrSplit.getDocBegin());
      solrQuery.setRows(numDocs);

      QueryResponse response;
      try {
        response = solr.query(solrQuery);
      } catch (final SolrServerException e) {
        throw new IOException(e);
      }

      final SolrDocumentList solrDocs = response.getResults();
      return new SolrRecordReader(solrDocs, numDocs);
    }
  }

  private Configuration conf;

  private SolrServer solr;

  private int numDeletes = 0;

  private UpdateRequest updateRequest = new UpdateRequest();

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void setup(Context job) throws IOException {
    Configuration conf = job.getConfiguration();
    try {
      solr = SolrUtils.getCommonsHttpSolrServer(conf);
    } catch (MalformedURLException e) {
      throw new IOException(e);
    }
  }


  @Override
  public void cleanup(Context context) throws IOException {
    try {
      if (numDeletes > 0) {
        updateRequest.process(solr);

        solr.commit();
      }
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void reduce(Text key, Iterable<SolrRecord> values, Context context)
  throws IOException {
    Iterator<SolrRecord> iterator = values.iterator();
    SolrRecord recordToKeep = iterator.next();
    while (iterator.hasNext()) {
      SolrRecord solrRecord = iterator.next();
      if (solrRecord.getBoost() > recordToKeep.getBoost() ||
          (solrRecord.getBoost() == recordToKeep.getBoost() && 
              solrRecord.getTstamp() > recordToKeep.getTstamp())) {
        updateRequest.deleteById(recordToKeep.id);
        recordToKeep = solrRecord;
      } else {
        updateRequest.deleteById(solrRecord.id);
      }
      numDeletes++;
      if (numDeletes >= NUM_MAX_DELETE_REQUEST) {
        try {
          updateRequest.process(solr);
        } catch (SolrServerException e) {
          throw new IOException(e);
        }
        updateRequest = new UpdateRequest();
        numDeletes = 0;
      }
    }
  }

  public boolean dedup(String solrUrl)
  throws IOException, InterruptedException, ClassNotFoundException {
    LOG.info("SolrDeleteDuplicates: starting...");
    LOG.info("SolrDeleteDuplicates: Solr url: " + solrUrl);
    
    getConf().set(SolrConstants.SERVER_URL, solrUrl);
    
    Job job = new Job(getConf(), "solrdedup");

    job.setInputFormatClass(SolrInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(SolrRecord.class);
    job.setMapperClass(Mapper.class);
    job.setReducerClass(SolrDeleteDuplicates.class);

    return job.waitForCompletion(true);    
  }

  public int run(String[] args)
  throws IOException, InterruptedException, ClassNotFoundException {
    if (args.length != 1) {
      System.err.println("Usage: SolrDeleteDuplicates <solr url>");
      return 1;
    }

    boolean result = dedup(args[0]);
    if (result) {
      LOG.info("SolrDeleteDuplicates: done.");
      return 0;
    }

    return -1;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(NutchConfiguration.create(),
        new SolrDeleteDuplicates(), args);
    System.exit(result);
  }

}
