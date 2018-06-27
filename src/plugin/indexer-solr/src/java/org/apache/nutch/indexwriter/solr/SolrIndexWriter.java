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
import java.net.URLDecoder;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.IndexerMapReduce;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrIndexWriter implements IndexWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private List<SolrClient> solrClients;
  private ModifiableSolrParams params;

  private Configuration config;

  private final List<SolrInputDocument> inputDocs = new ArrayList<>();

  private final List<String> deleteIds = new ArrayList<>();

  private int batchSize;
  private int numDeletes = 0;
  private int totalAdds = 0;
  private int totalDeletes = 0;
  private boolean delete = false;
  private String weightField;

  private boolean auth;
  private String username;
  private String password;

  @Override
  public void open(Configuration conf, String name) {
    //Implementation not required
  }

  /**
   * Initializes the internal variables from a given index writer configuration.
   *
   * @param parameters Params from the index writer configuration.
   */
  @Override
  public void open(IndexWriterParams parameters) {
    String type = parameters.get(SolrConstants.SERVER_TYPE, "http");

    String[] urls = parameters.getStrings(SolrConstants.SERVER_URLS);

    if (urls == null) {
      String message = "Missing SOLR URL.\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    this.auth = parameters.getBoolean(SolrConstants.USE_AUTH, false);
    this.username = parameters.get(SolrConstants.USERNAME);
    this.password = parameters.get(SolrConstants.PASSWORD);

    this.solrClients = new ArrayList<>();

    switch (type) {
    case "http":
      for (String url : urls) {
        solrClients.add(SolrUtils.getHttpSolrClient(url));
      }
      break;
    case "cloud":
      CloudSolrClient sc = this.auth ?
          SolrUtils.getCloudSolrClient(Arrays.asList(urls), this.username,
              this.password) :
          SolrUtils.getCloudSolrClient(Arrays.asList(urls));
      sc.setDefaultCollection(parameters.get(SolrConstants.COLLECTION));
      solrClients.add(sc);
      break;
    case "concurrent":
      // TODO: 1/08/17 Implement this
      throw new UnsupportedOperationException(
          "The type \"concurrent\" is not yet supported.");
    case "lb":
      // TODO: 1/08/17 Implement this
      throw new UnsupportedOperationException(
          "The type \"lb\" is not yet supported.");
    default:
      throw new IllegalArgumentException(
          "The type \"" + type + "\" is not supported.");
    }

    init(parameters);
  }

  private void init(IndexWriterParams properties) {
    batchSize = properties.getInt(SolrConstants.COMMIT_SIZE, 1000);
    delete = config.getBoolean(IndexerMapReduce.INDEXER_DELETE, false);
    weightField = properties.get(SolrConstants.WEIGHT_FIELD, "");

    // parse optional params
    params = new ModifiableSolrParams();
    String paramString = config.get(IndexerMapReduce.INDEXER_PARAMS);
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
      LOG.warn("Could not decode: " + key
          + ", it probably wasn't encoded in the first place..");
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
          val2 = DateTimeFormatter.ISO_INSTANT.format(((Date) val).toInstant());
        }

        if (e.getKey().equals("content") || e.getKey().equals("title")) {
          val2 = SolrUtils.stripNonCharCodepoints((String) val);
        }

        inputDoc.addField(e.getKey(), val2);
      }
    }

    if (!weightField.isEmpty()) {
      inputDoc.addField(weightField, doc.getWeight());
    }
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
        if (this.auth) {
          UpdateRequest req = new UpdateRequest();
          req.setAction(UpdateRequest.ACTION.COMMIT, true, true);
          req.setBasicAuthCredentials(this.username, this.password);
          solrClient.request(req);
        } else {
          solrClient.commit();
        }
      }
    } catch (final SolrServerException e) {
      LOG.error("Failed to commit solr connection: " + e.getMessage());
    }
  }

  private void push() throws IOException {
    if (inputDocs.size() > 0) {
      try {
        LOG.info(
            "Indexing " + Integer.toString(inputDocs.size()) + "/" + Integer
                .toString(totalAdds) + " documents");
        LOG.info("Deleting " + Integer.toString(numDeletes) + " documents");
        numDeletes = 0;
        UpdateRequest req = new UpdateRequest();
        req.add(inputDocs);
        req.setAction(UpdateRequest.ACTION.OPTIMIZE, false, false);
        req.setParams(params);
        if (this.auth) {
          req.setBasicAuthCredentials(this.username, this.password);
        }
        for (SolrClient solrClient : solrClients) {
          solrClient.request(req);
        }
      } catch (final SolrServerException e) {
        throw makeIOException(e);
      }
      inputDocs.clear();
    }

    if (deleteIds.size() > 0) {
      try {
        LOG.info(
            "SolrIndexer: deleting " + Integer.toString(deleteIds.size()) + "/"
                + Integer.toString(totalDeletes) + " documents");
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

  private static IOException makeIOException(SolrServerException e) {
    return new IOException(e);
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
  }

  /**
   * Returns a String describing the IndexWriter instance and the specific parameters it can take.
   *
   * @return The full description.
   */
  @Override
  public String describe() {
    StringBuffer sb = new StringBuffer("SOLRIndexWriter\n");
    sb.append("\t").append(SolrConstants.SERVER_TYPE).append(
        " : Type of the server. Can be: \"cloud\", \"concurrent\", \"http\" or \"lb\"\n");
    sb.append("\t").append(SolrConstants.SERVER_URLS)
        .append(" : URL of the SOLR instance or URL of the Zookeeper quorum\n");
    sb.append("\t").append(SolrConstants.COMMIT_SIZE)
        .append(" : buffer size when sending to SOLR (default 1000)\n");
    sb.append("\t").append(SolrConstants.USE_AUTH)
        .append(" : use authentication (default false)\n");
    sb.append("\t").append(SolrConstants.USERNAME)
        .append(" : username for authentication\n");
    sb.append("\t").append(SolrConstants.PASSWORD)
        .append(" : password for authentication\n");
    return sb.toString();
  }
}
