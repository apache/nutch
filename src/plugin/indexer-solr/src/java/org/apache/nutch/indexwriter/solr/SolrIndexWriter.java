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
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexerMapReduce;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// WORK AROUND FOR NOT REMOVING URL ENCODED URLS!!!
import java.net.URLDecoder;

public class SolrIndexWriter implements IndexWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private List<SolrClient> solrClients;
  private SolrMappingReader solrMapping;
  private ModifiableSolrParams params;

  private Configuration config;

  private final List<SolrInputDocument> inputDocs = new ArrayList<SolrInputDocument>();

  private final List<SolrInputDocument> updateDocs = new ArrayList<SolrInputDocument>();
    
  private final List<String> deleteIds = new ArrayList<String>();

  private int batchSize;
  private int numDeletes = 0;
  private int totalAdds = 0;
  private int totalDeletes = 0;
  private int totalUpdates = 0;
  private boolean delete = false;

  public void open(Configuration conf, String name) throws IOException {
    solrClients = SolrUtils.getSolrClients(conf);
    init(solrClients, conf);
  }

  // package protected for tests
  void init(List<SolrClient> solrClients, Configuration conf) throws IOException {
    batchSize = conf.getInt(SolrConstants.COMMIT_SIZE, 1000);
    solrMapping = SolrMappingReader.getInstance(conf);
    delete = conf.getBoolean(IndexerMapReduce.INDEXER_DELETE, false);
    // parse optional params
    params = new ModifiableSolrParams();
    String paramString = conf.get(IndexerMapReduce.INDEXER_PARAMS);
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
    try {
      key = URLDecoder.decode(key, "UTF8");
    } catch (UnsupportedEncodingException e) {
      LOG.error("Error decoding: " + key);
      throw new IOException("UnsupportedEncodingException for " + key);
    } catch (IllegalArgumentException e) {
      LOG.warn("Could not decode: " + key + ", it probably wasn't encoded in the first place..");
    }
    
    // escape solr hash separator
    key = key.replaceAll("!", "\\!");
    
    if (delete) {
      deleteIds.add(key);
      totalDeletes++;
    }
    
    if (deleteIds.size() >= batchSize) {
      push();
    }

  }

  public void deleteByQuery(String query) throws IOException {
    try {
      LOG.info("SolrWriter: deleting " + query);
      for (SolrClient solrClient : solrClients) {
        solrClient.deleteByQuery(query);
      }
    } catch (final SolrServerException e) {
      LOG.error("Error deleting: " + deleteIds);
      throw makeIOException(e);
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
    totalAdds++;

    if (inputDocs.size() + numDeletes >= batchSize) {
      push();
    }
  }

  public void close() throws IOException {
    commit();

    for (SolrClient solrClient : solrClients) {
      solrClient.close();
    }
  }

  @Override
  public void commit() throws IOException {
    push();
    try {
      for (SolrClient solrClient : solrClients) {
        solrClient.commit();
      }
    } catch (final SolrServerException e) {
      LOG.error("Failed to commit solr connection: " + e.getMessage()); // FIXME
    }
  }
    
  public void push() throws IOException {
    if (inputDocs.size() > 0) {
      try {
        LOG.info("Indexing " + Integer.toString(inputDocs.size())
            + "/" + Integer.toString(totalAdds) + " documents");
        LOG.info("Deleting " + Integer.toString(numDeletes) + " documents");
        numDeletes = 0;
        UpdateRequest req = new UpdateRequest();
        req.add(inputDocs);
        req.setAction(AbstractUpdateRequest.ACTION.OPTIMIZE, false, false);
        req.setParams(params);
        for (SolrClient solrClient : solrClients) {
          NamedList res = solrClient.request(req);
        }
      } catch (final SolrServerException e) {
        throw makeIOException(e);
      }
      inputDocs.clear();
    }

    if (deleteIds.size() > 0) {
      try {
        LOG.info("SolrIndexer: deleting " + Integer.toString(deleteIds.size()) 
            + "/" + Integer.toString(totalDeletes) + " documents");
        for (SolrClient solrClient : solrClients) {
          solrClient.deleteById(deleteIds);
        }
      } catch (final SolrServerException e) {
        LOG.error("Error deleting: " + deleteIds);
        throw makeIOException(e);
      }
      deleteIds.clear();
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
    String zkHosts = conf.get(SolrConstants.ZOOKEEPER_HOSTS);
    if (serverURL == null && zkHosts == null) {
      String message = "Missing SOLR URL and Zookeeper URL. Either on should be set via -D "
          + SolrConstants.SERVER_URL + " or -D " + SolrConstants.ZOOKEEPER_HOSTS;
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }
  }

  public String describe() {
    StringBuffer sb = new StringBuffer("SOLRIndexWriter\n");
    sb.append("\t").append(SolrConstants.SERVER_URL)
        .append(" : URL of the SOLR instance\n");
    sb.append("\t").append(SolrConstants.ZOOKEEPER_HOSTS)
        .append(" : URL of the Zookeeper quorum\n");
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
