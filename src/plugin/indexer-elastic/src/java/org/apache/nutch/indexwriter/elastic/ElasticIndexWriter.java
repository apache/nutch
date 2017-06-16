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

package org.apache.nutch.indexwriter.elastic;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.lang.invoke.MethodHandles;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.NutchDocument;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends NutchDocuments to a configured Elasticsearch index.
 */
public class ElasticIndexWriter implements IndexWriter {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final int DEFAULT_PORT = 9300;
  private static final int DEFAULT_MAX_BULK_DOCS = 250;
  private static final int DEFAULT_MAX_BULK_LENGTH = 2500500;
  private static final int DEFAULT_EXP_BACKOFF_MILLIS = 100;
  private static final int DEFAULT_EXP_BACKOFF_RETRIES = 10;
  private static final int DEFAULT_BULK_CLOSE_TIMEOUT = 600;
  private static final String DEFAULT_INDEX = "nutch";

  private String defaultIndex;
  private Client client;
  private Node node;
  private BulkProcessor bulkProcessor;

  private long bulkCloseTimeout;

  private Configuration config;

  @Override
  public void open(JobConf job, String name) throws IOException {
    bulkCloseTimeout = job.getLong(ElasticConstants.BULK_CLOSE_TIMEOUT,
        DEFAULT_BULK_CLOSE_TIMEOUT);
    defaultIndex = job.get(ElasticConstants.INDEX, DEFAULT_INDEX);

    int maxBulkDocs = job.getInt(ElasticConstants.MAX_BULK_DOCS,
        DEFAULT_MAX_BULK_DOCS);
    int maxBulkLength = job.getInt(ElasticConstants.MAX_BULK_LENGTH,
        DEFAULT_MAX_BULK_LENGTH);
    int expBackoffMillis = job.getInt(ElasticConstants.EXPONENTIAL_BACKOFF_MILLIS,
        DEFAULT_EXP_BACKOFF_MILLIS);
    int expBackoffRetries = job.getInt(ElasticConstants.EXPONENTIAL_BACKOFF_RETRIES,
        DEFAULT_EXP_BACKOFF_RETRIES);

    client = makeClient(job);

    LOG.debug("Creating BulkProcessor with maxBulkDocs={}, maxBulkLength={}", maxBulkDocs, maxBulkLength);
    bulkProcessor = BulkProcessor.builder(client, bulkProcessorListener())
      .setBulkActions(maxBulkDocs)
      .setBulkSize(new ByteSizeValue(maxBulkLength, ByteSizeUnit.BYTES))
      .setConcurrentRequests(1)
      .setBackoffPolicy(BackoffPolicy.exponentialBackoff(
          TimeValue.timeValueMillis(expBackoffMillis), expBackoffRetries))
      .build();
  }

  /**
   * Initializes the internal variables from a given index writer configuration.
   *
   * @param parameters Params from the index writer configuration.
   * @throws IOException Some exception thrown by writer.
   */
  @Override
  public void open(Map<String, String> parameters) throws IOException {

  }

  /** Generates a TransportClient or NodeClient */
  protected Client makeClient(Configuration conf) throws IOException {
    String clusterName = conf.get(ElasticConstants.CLUSTER);
    String[] hosts = conf.getStrings(ElasticConstants.HOSTS);
    int port = conf.getInt(ElasticConstants.PORT, DEFAULT_PORT);

    Settings.Builder settingsBuilder = Settings.settingsBuilder();

    BufferedReader reader = new BufferedReader(
        conf.getConfResourceAsReader("elasticsearch.conf"));
    String line;
    String parts[];
    while ((line = reader.readLine()) != null) {
      if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
        line.trim();
        parts = line.split("=");

        if (parts.length == 2) {
          settingsBuilder.put(parts[0].trim(), parts[1].trim());
        }
      }
    }

    // Set the cluster name and build the settings
    if (StringUtils.isNotBlank(clusterName))
      settingsBuilder.put("cluster.name", clusterName);

    Settings settings = settingsBuilder.build();

    Client client = null;

    // Prefer TransportClient
    if (hosts != null && port > 1) {
      TransportClient transportClient = TransportClient.builder().settings(settings).build();
      for (String host: hosts)
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
      client = transportClient;
    } else if (clusterName != null) {
      node = nodeBuilder().settings(settings).client(true).node();
      client = node.client();
    }

    return client;
  }

  /** Generates a default BulkProcessor.Listener */
  protected BulkProcessor.Listener bulkProcessorListener() {
    return new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) { }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        throw new RuntimeException(failure);
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        if (response.hasFailures()) {
          LOG.warn("Failures occurred during bulk request");
        }
      }
    };
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    String id = (String) doc.getFieldValue("id");
    String type = doc.getDocumentMeta().get("type");
    if (type == null)
      type = "doc";

    // Add each field of this doc to the index source
    Map<String, Object> source = new HashMap<String, Object>();
    for (String fieldName : doc.getFieldNames()) {
      if (doc.getFieldValue(fieldName) != null) {
        source.put(fieldName, doc.getFieldValue(fieldName));
      }
    }

    IndexRequest request = new IndexRequest(defaultIndex, type, id).source(source);
    bulkProcessor.add(request);
  }

  @Override
  public void delete(String key) throws IOException {
    DeleteRequest request = new DeleteRequest(defaultIndex, "doc", key);
    bulkProcessor.add(request);
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    write(doc);
  }

  @Override
  public void commit() throws IOException {
    bulkProcessor.flush();
  }

  @Override
  public void close() throws IOException {
    // Close BulkProcessor (automatically flushes)
    try {
      bulkProcessor.awaitClose(bulkCloseTimeout, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("interrupted while waiting for BulkProcessor to complete ({})", e.getMessage());
    }

    client.close();
    if (node != null) {
      node.close();
    }
  }

  @Override
  public String describe() {
    StringBuffer sb = new StringBuffer("ElasticIndexWriter\n");
    sb.append("\t").append(ElasticConstants.CLUSTER)
        .append(" : elastic prefix cluster\n");
    sb.append("\t").append(ElasticConstants.HOSTS).append(" : hostname\n");
    sb.append("\t").append(ElasticConstants.PORT).append(" : port\n");
    sb.append("\t").append(ElasticConstants.INDEX)
        .append(" : elastic index command \n");
    sb.append("\t").append(ElasticConstants.MAX_BULK_DOCS)
        .append(" : elastic bulk index doc counts. (default ")
        .append(DEFAULT_MAX_BULK_DOCS).append(")\n");
    sb.append("\t").append(ElasticConstants.MAX_BULK_LENGTH)
        .append(" : elastic bulk index length in bytes. (default ")
        .append(DEFAULT_MAX_BULK_LENGTH).append(")\n");
    sb.append("\t").append(ElasticConstants.EXPONENTIAL_BACKOFF_MILLIS)
        .append(" : elastic bulk exponential backoff initial delay in milliseconds. (default ")
        .append(DEFAULT_EXP_BACKOFF_MILLIS).append(")\n");
    sb.append("\t").append(ElasticConstants.EXPONENTIAL_BACKOFF_RETRIES)
        .append(" : elastic bulk exponential backoff max retries. (default ")
        .append(DEFAULT_EXP_BACKOFF_RETRIES).append(")\n");
    sb.append("\t").append(ElasticConstants.BULK_CLOSE_TIMEOUT)
        .append(" : elastic timeout for the last bulk in seconds. (default ")
        .append(DEFAULT_BULK_CLOSE_TIMEOUT).append(")\n");
    return sb.toString();
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
    String cluster = conf.get(ElasticConstants.CLUSTER);
    String hosts = conf.get(ElasticConstants.HOSTS);

    if (StringUtils.isBlank(cluster) && StringUtils.isBlank(hosts)) {
      String message = "Missing elastic.cluster and elastic.host. At least one of them should be set in nutch-site.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }
  }

  @Override
  public Configuration getConf() {
    return config;
  }
}
