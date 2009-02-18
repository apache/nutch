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
    inputDoc.setDocumentBoost(doc.getScore());
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
