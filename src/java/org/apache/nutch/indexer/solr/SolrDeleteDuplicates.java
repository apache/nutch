package org.apache.nutch.indexer.solr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.DeleteDuplicates;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
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
 * Note that unlike {@link DeleteDuplicates} we assume that two documents in
 * a solr index will never have the same URL. So this class only deals with
 * documents with <b>different</b> URLs but the same digest. 
 */
public class SolrDeleteDuplicates
implements Reducer<Text, SolrDeleteDuplicates.SolrRecord, Text, SolrDeleteDuplicates.SolrRecord>,
Tool {

  public static final Log LOG = LogFactory.getLog(SolrDeleteDuplicates.class);

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
      tstamp = (Long)doc.getFieldValue(SolrConstants.TIMESTAMP_FIELD);
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

  public static class SolrInputSplit implements InputSplit {

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

    public int getNumDocs() {
      return numDocs;
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

  public static class SolrInputFormat implements InputFormat<Text, SolrRecord> {

    /** Return each index as a split. */
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
      SolrServer solr = new CommonsHttpSolrServer(job.get(SolrConstants.SERVER_URL));

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
      SolrInputSplit[] splits = new SolrInputSplit[numSplits];
      for (int i = 0; i < numSplits - 1; i++) {
        splits[i] = new SolrInputSplit(currentDoc, numDocsPerSplit);
        currentDoc += numDocsPerSplit;
      }
      splits[splits.length - 1] = new SolrInputSplit(currentDoc, numResults - currentDoc);

      return splits;
    }

    public RecordReader<Text, SolrRecord> getRecordReader(final InputSplit split,
        final JobConf job, 
        Reporter reporter)
        throws IOException {

      SolrServer solr = new CommonsHttpSolrServer(job.get(SolrConstants.SERVER_URL));
      SolrInputSplit solrSplit = (SolrInputSplit) split;
      final int numDocs = solrSplit.getNumDocs();
      
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

      return new RecordReader<Text, SolrRecord>() {

        private int currentDoc = 0;

        @Override
        public void close() throws IOException { }

        @Override
        public Text createKey() {
          return new Text();
        }

        @Override
        public SolrRecord createValue() {
          return new SolrRecord();
        }

        @Override
        public long getPos() throws IOException {
          return currentDoc;
        }

        @Override
        public float getProgress() throws IOException {
          return currentDoc / (float) numDocs;
        }

        @Override
        public boolean next(Text key, SolrRecord value) throws IOException {
          if (currentDoc >= numDocs) {
            return false;
          }

          SolrDocument doc = solrDocs.get(currentDoc);
          String digest = (String) doc.getFieldValue(SolrConstants.DIGEST_FIELD);
          key.set(digest);
          value.readSolrDocument(doc);

          currentDoc++;
          return true;
        }    
      };
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
  public void configure(JobConf job) {
    try {
      solr = new CommonsHttpSolrServer(job.get(SolrConstants.SERVER_URL));
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public void close() throws IOException {
    try {
      if (numDeletes > 0) {
        updateRequest.process(solr);
      }
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void reduce(Text key, Iterator<SolrRecord> values,
      OutputCollector<Text, SolrRecord> output, Reporter reporter)
  throws IOException {
    SolrRecord recordToKeep = values.next();
    while (values.hasNext()) {
      SolrRecord solrRecord = values.next();
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

  public void dedup(String solrUrl) throws IOException {
    LOG.info("SolrDeleteDuplicates: starting...");
    LOG.info("SolrDeleteDuplicates: Solr url: " + solrUrl);
    
    JobConf job = new NutchJob(getConf());

    job.set(SolrConstants.SERVER_URL, solrUrl);
    job.setInputFormat(SolrInputFormat.class);
    job.setOutputFormat(NullOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(SolrRecord.class);
    job.setMapperClass(IdentityMapper.class);
    job.setReducerClass(SolrDeleteDuplicates.class);

    JobClient.runJob(job);
    
    LOG.info("SolrDeleteDuplicates: done.");
  }

  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: SolrDeleteDuplicates <solr url>");
      return 1;
    }

    dedup(args[0]);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(NutchConfiguration.create(),
        new SolrDeleteDuplicates(), args);
    System.exit(result);
  }

}
