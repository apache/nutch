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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexerMapReduce;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrIndexWriter implements IndexWriter {

  public static final Logger LOG = LoggerFactory
      .getLogger(SolrIndexWriter.class);

  private SolrServer solr;
  private SolrMappingReader solrMapping;
  private ModifiableSolrParams params;

  private Configuration config;

  private final List<SolrInputDocument> inputDocs = new ArrayList<SolrInputDocument>();

  private int batchSize;
  private int numDeletes = 0;
  private boolean delete = false;

  public void open(JobConf job, String name) throws IOException {
    SolrServer server = SolrUtils.getSolrServer(job);
    init(server, job);
  }

  // package protected for tests
  void init(SolrServer server, JobConf job) throws IOException {
    solr = server;
    batchSize = job.getInt(SolrConstants.COMMIT_SIZE, 1000);
    solrMapping = SolrMappingReader.getInstance(job);
    delete = job.getBoolean(IndexerMapReduce.INDEXER_DELETE, false);
    // parse optional params
    params = new ModifiableSolrParams();
    String paramString = job.get(IndexerMapReduce.INDEXER_PARAMS);
    if (paramString != null) {
      String[] values = paramString.split("&");
      for (String v : values) {
        String[] kv = v.split("=");
        if (kv.length < 2) {
          continue;
        }
        params.add(kv[0], kv[1]);
      }
    }
  }

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

  public void write(NutchDocument doc) throws IOException {
    final SolrInputDocument inputDoc = new SolrInputDocument();
    for (final Entry<String, NutchField> e : doc) {
      for (final Object val : e.getValue().getValues()) {
        // normalise the string representation for a Date
        Object val2 = val;

        if (val instanceof Date) {
          val2 = DateUtil.getThreadLocalDateFormat().format(val);
        }

        if (e.getKey().equals("content") || e.getKey().equals("title")) {
          val2 = SolrUtils.stripNonCharCodepoints((String) val);
        }

        inputDoc.addField(solrMapping.mapKey(e.getKey()), val2, e.getValue()
            .getWeight());
        String sCopy = solrMapping.mapCopyKey(e.getKey());
        if (sCopy != e.getKey()) {
          inputDoc.addField(sCopy, val);
        }
      }
    }

    inputDoc.setDocumentBoost(doc.getWeight());
    inputDocs.add(inputDoc);
    if (inputDocs.size() + numDeletes >= batchSize) {
      try {
        LOG.info("Indexing " + Integer.toString(inputDocs.size())
            + " documents");
        LOG.info("Deleting " + Integer.toString(numDeletes) + " documents");
        numDeletes = 0;
        UpdateRequest req = new UpdateRequest();
        req.add(inputDocs);
        req.setParams(params);
        req.process(solr);
      } catch (final SolrServerException e) {
        throw makeIOException(e);
      }
      inputDocs.clear();
    }
  }

  public void close() throws IOException {
    try {
      if (!inputDocs.isEmpty()) {
        LOG.info("Indexing " + Integer.toString(inputDocs.size())
            + " documents");
        if (numDeletes > 0) {
          LOG.info("Deleting " + Integer.toString(numDeletes) + " documents");
        }
        UpdateRequest req = new UpdateRequest();
        req.add(inputDocs);
        req.setParams(params);
        req.process(solr);
        inputDocs.clear();
      }
    } catch (final SolrServerException e) {
      throw makeIOException(e);
    }
  }

  @Override
  public void commit() throws IOException {
    try {
      solr.commit();
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
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
    String serverURL = conf.get(SolrConstants.SERVER_URL);
    if (serverURL == null) {
      String message = "Missing Solr URL. Should be set via -D "
          + SolrConstants.SERVER_URL;
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }
  }

  public String describe() {
    StringBuffer sb = new StringBuffer("SolrIndexWriter\n");
    sb.append("\t").append(SolrConstants.SERVER_TYPE)
        .append(" : Type of SolrServer to communicate with (default 'http' however options include 'cloud', 'lb' and 'concurrent')\n");
    sb.append("\t").append(SolrConstants.SERVER_URL)
        .append(" : URL of the Solr instance (mandatory)\n");
    sb.append("\t").append(SolrConstants.ZOOKEEPER_URL)
        .append(" : URL of the Zookeeper URL (mandatory if 'cloud' value for solr.server.type)\n");
    sb.append("\t").append(SolrConstants.LOADBALANCE_URLS)
        .append(" : Comma-separated string of Solr server strings to be used (madatory if 'lb' value for solr.server.type)\n");
    sb.append("\t")
        .append(SolrConstants.MAPPING_FILE)
        .append(" : name of the mapping file for fields (default solrindex-mapping.xml)\n");
    sb.append("\t").append(SolrConstants.COMMIT_SIZE)
        .append(" : buffer size when sending to Solr (default 1000)\n");
    sb.append("\t").append(SolrConstants.USE_AUTH)
        .append(" : use authentication (default false)\n");
    sb.append("\t").append(SolrConstants.USERNAME)
        .append(" : username for authentication\n");
    sb.append("\t").append(SolrConstants.PASSWORD)
        .append(" : password for authentication\n");
    return sb.toString();
  }
}
