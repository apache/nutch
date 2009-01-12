package org.apache.nutch.indexer.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchIndexWriter;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class SolrWriter implements NutchIndexWriter {

  private SolrServer solr;

  private final List<SolrInputDocument> inputDocs =
    new ArrayList<SolrInputDocument>();

  private int commitSize;

  public void open(JobConf job, String name)
  throws IOException {
    solr = new CommonsHttpSolrServer(job.get(SolrConstants.SERVER_URL));
    commitSize = job.getInt(SolrConstants.COMMIT_SIZE, 1000);
  }

  public void write(NutchDocument doc) throws IOException {
    final SolrInputDocument inputDoc = new SolrInputDocument();
    for(final Entry<String, List<String>> e : doc) {
      for (final String val : e.getValue()) {
        inputDoc.addField(e.getKey(), val);
      }
    }
    inputDocs.add(inputDoc);
    if (inputDocs.size() > commitSize) {
      try {
        solr.add(inputDocs);
      } catch (final SolrServerException e) {
        throw makeIOException(e);
      }
      inputDocs.clear();
    }
  }

  public void close() throws IOException {
    try {
      if (!inputDocs.isEmpty()) {
        solr.add(inputDocs);
        inputDocs.clear();
      }
      solr.commit();
    } catch (final SolrServerException e) {
      throw makeIOException(e);
    }
  }

  public static IOException makeIOException(SolrServerException e) {
    final IOException ioe = new IOException();
    ioe.initCause(e);
    return ioe;
  }

}
