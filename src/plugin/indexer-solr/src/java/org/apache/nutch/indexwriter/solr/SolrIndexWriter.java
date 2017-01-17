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
package org.apache.nutch.indexwriter.solr;

import java.lang.invoke.MethodHandles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.CleaningJob;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class SolrIndexWriter implements IndexWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private HttpSolrServer solr;
  private SolrMappingReader solrMapping;

  private Configuration config;

  private final List<SolrInputDocument> inputDocs = new ArrayList<SolrInputDocument>();

  private int batchSize;
  private int numDeletes = 0;
  private boolean delete = false;

  protected static long documentCount = 0;

  @Override
  public void open(Configuration conf) throws IOException {
    solr = SolrUtils.getHttpSolrServer(conf);
    batchSize = conf.getInt(SolrConstants.COMMIT_SIZE, 1000);
    solrMapping = SolrMappingReader.getInstance(conf);
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    final SolrInputDocument inputDoc = new SolrInputDocument();
    for (final Entry<String, List<String>> e : doc) {
      for (final String val : e.getValue()) {

        Object val2 = val;
        if (e.getKey().equals("content") || e.getKey().equals("title")) {
          val2 = SolrUtils.stripNonCharCodepoints(val);
        }

        inputDoc.addField(solrMapping.mapKey(e.getKey()), val2);
        String sCopy = solrMapping.mapCopyKey(e.getKey());
        if (sCopy != e.getKey()) {
          inputDoc.addField(sCopy, val2);
        }
      }
    }
    inputDoc.setDocumentBoost(doc.getScore());
    inputDocs.add(inputDoc);
    documentCount++;
    if (inputDocs.size() >= batchSize) {
      try {
        LOG.info("Adding " + Integer.toString(inputDocs.size()) + " documents");
        solr.add(inputDocs);
      } catch (final SolrServerException e) {
        throw new IOException(e);
      }
      inputDocs.clear();
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (!inputDocs.isEmpty()) {
        LOG.info("Adding " + Integer.toString(inputDocs.size()) + " documents");
        solr.add(inputDocs);
        inputDocs.clear();
      } else if (numDeletes > 0) {
        LOG.info("Deleted " + Integer.toString(numDeletes) + " documents");
      }
    } catch (final SolrServerException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
    String serverURL = conf.get(SolrConstants.SERVER_URL);
    delete = config.getBoolean(CleaningJob.ARG_COMMIT, false);
    if (serverURL == null) {
      String message = "Missing SOLR URL. Should be set via -D "
          + SolrConstants.SERVER_URL;
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }
  }

  @Override
  public void delete(String key) throws IOException {
    if (delete) {
      try {
        solr.deleteById(key);
        numDeletes++;
      } catch (final SolrServerException e) {
        throw makeIOException(e);
      }
    }
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    write(doc);
  }

  @Override
  public void commit() throws IOException {
    try {
      solr.commit();
      LOG.info("Total " + documentCount
          + (documentCount > 1 ? " documents are " : " document is ")
          + "added.");
    } catch (SolrServerException e) {
      throw makeIOException(e);
    }
  }

  public static IOException makeIOException(SolrServerException e) {
    final IOException ioe = new IOException();
    ioe.initCause(e);
    return ioe;
  }

  @Override
  public String describe() {
    StringBuffer sb = new StringBuffer("SOLRIndexWriter\n");
    sb.append("\t").append(SolrConstants.SERVER_URL)
        .append(" : URL of the SOLR instance (mandatory)\n");
    sb.append("\t").append(SolrConstants.COMMIT_SIZE)
        .append(" : buffer size when sending to SOLR (default 1000)\n");
    sb.append("\t")
        .append(SolrConstants.MAPPING_FILE)
        .append(
            " : name of the mapping file for fields (default solrindex-mapping.xml)\n");
    sb.append("\t").append(SolrConstants.USE_AUTH)
        .append(" : use authentication (default false)\n");
    sb.append("\t").append(SolrConstants.USERNAME)
        .append(" : username for authentication\n");
    sb.append("\t").append(SolrConstants.PASSWORD)
        .append(" : password for authentication\n");
    return sb.toString();
  }

}
