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

import java.lang.invoke.MethodHandles;
import java.time.format.DateTimeFormatter;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.client.RequestOptions;

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
  private static final String DEFAULT_USER = "elastic";

  private String[] hosts;
  private int port;
  private Boolean https = null;
  private String user = null;
  private String password = null;
  private Boolean auth;

  private int maxBulkDocs;
  private int maxBulkLength;
  private int expBackoffMillis;
  private int expBackoffRetries;

  private String defaultIndex;
  private RestHighLevelClient client;
  private BulkProcessor bulkProcessor;

  private long bulkCloseTimeout;

  private Configuration config;

  @Override
  public void open(Configuration conf, String name) throws IOException {
    // Implementation not required
  }

  /**
   * Initializes the internal variables from a given index writer configuration.
   *
   * @param parameters
   *          Params from the index writer configuration.
   * @throws IOException
   *           Some exception thrown by writer.
   */
  @Override
  public void open(IndexWriterParams parameters) throws IOException {

    String hosts = parameters.get(ElasticConstants.HOSTS);

    if (StringUtils.isBlank(hosts)) {
      String message = "Missing elastic.host this should be set in index-writers.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    bulkCloseTimeout = parameters.getLong(ElasticConstants.BULK_CLOSE_TIMEOUT,
        DEFAULT_BULK_CLOSE_TIMEOUT);
    defaultIndex = parameters.get(ElasticConstants.INDEX, DEFAULT_INDEX);

    maxBulkDocs = parameters.getInt(ElasticConstants.MAX_BULK_DOCS,
        DEFAULT_MAX_BULK_DOCS);
    maxBulkLength = parameters.getInt(ElasticConstants.MAX_BULK_LENGTH,
        DEFAULT_MAX_BULK_LENGTH);
    expBackoffMillis = parameters.getInt(
        ElasticConstants.EXPONENTIAL_BACKOFF_MILLIS,
        DEFAULT_EXP_BACKOFF_MILLIS);
    expBackoffRetries = parameters.getInt(
        ElasticConstants.EXPONENTIAL_BACKOFF_RETRIES,
        DEFAULT_EXP_BACKOFF_RETRIES);

    client = makeClient(parameters);

    LOG.debug("Creating BulkProcessor with maxBulkDocs={}, maxBulkLength={}",
        maxBulkDocs, maxBulkLength);
    bulkProcessor = BulkProcessor
        .builder((request, bulkListener) -> client.bulkAsync(request,
            RequestOptions.DEFAULT, bulkListener), bulkProcessorListener())
        .setBulkActions(maxBulkDocs)
        .setBulkSize(new ByteSizeValue(maxBulkLength, ByteSizeUnit.BYTES))
        .setConcurrentRequests(1)
        .setBackoffPolicy(BackoffPolicy.exponentialBackoff(
            TimeValue.timeValueMillis(expBackoffMillis), expBackoffRetries))
        .build();
  }

  /**
   * Generates a RestHighLevelClient with the hosts given
   * @param parameters implementation specific {@link org.apache.nutch.indexer.IndexWriterParams}
   * @return an initialized {@link org.elasticsearch.client.RestHighLevelClient}
   * @throws IOException if there is an error reading the 
   * {@link org.apache.nutch.indexer.IndexWriterParams}
   */
  protected RestHighLevelClient makeClient(IndexWriterParams parameters)
      throws IOException {
    hosts = parameters.getStrings(ElasticConstants.HOSTS);
    port = parameters.getInt(ElasticConstants.PORT, DEFAULT_PORT);

    auth = parameters.getBoolean(ElasticConstants.USE_AUTH, false);
    user = parameters.get(ElasticConstants.USER, DEFAULT_USER);
    password = parameters.get(ElasticConstants.PASSWORD, "");

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(user, password));

    RestHighLevelClient client = null;

    if (hosts != null && port > 1) {
      HttpHost[] hostsList = new HttpHost[hosts.length];
      int i = 0;
      for (String host : hosts) {
        hostsList[i++] = new HttpHost(host, port);
      }
      RestClientBuilder restClientBuilder = RestClient.builder(hostsList);
      if (auth) {
        restClientBuilder
            .setHttpClientConfigCallback(new HttpClientConfigCallback() {
              @Override
              public HttpAsyncClientBuilder customizeHttpClient(
                  HttpAsyncClientBuilder arg0) {
                return arg0.setDefaultCredentialsProvider(credentialsProvider);
              }
            });
      }
      client = new RestHighLevelClient(restClientBuilder);
    } else {
      throw new IOException(
          "ElasticRestClient initialization Failed!!!\\n\\nPlease Provide the hosts");
    }

    return client;
  }

  /**
   * Generates a default BulkProcessor.Listener
   * @return {@link BulkProcessor.Listener}
   */
  protected BulkProcessor.Listener bulkProcessorListener() {
    return new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
          Throwable failure) {
        LOG.error("Elasticsearch indexing failed:", failure);
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request,
          BulkResponse response) {
        if (response.hasFailures()) {
          LOG.warn("Failures occurred during bulk request: {}",
              response.buildFailureMessage());
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

    // Add each field of this doc to the index builder
    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
    for (final Map.Entry<String, NutchField> e : doc) {
      final List<Object> values = e.getValue().getValues();

      if (values.size() > 1) {
        builder.array(e.getKey(), values);
      } else {
        Object value = values.get(0);
        if (value instanceof java.util.Date) {
          value = DateTimeFormatter.ISO_INSTANT
              .format(((java.util.Date) value).toInstant());
        }
        builder.field(e.getKey(), value);
      }
    }
    builder.endObject();

    IndexRequest request = new IndexRequest(defaultIndex).id(id)
        .source(builder);
    request.opType(DocWriteRequest.OpType.INDEX);

    bulkProcessor.add(request);
  }

  @Override
  public void delete(String key) throws IOException {
    DeleteRequest request = new DeleteRequest(defaultIndex, key);
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
      LOG.warn("interrupted while waiting for BulkProcessor to complete ({})",
          e.getMessage());
    }

    client.close();
  }

  /**
   * Returns {@link Map} with the specific parameters the IndexWriter instance
   * can take.
   *
   * @return The values of each row. It must have the form
   *         &#60;KEY,&#60;DESCRIPTION,VALUE&#62;&#62;.
   */
  @Override
  public Map<String, Map.Entry<String, Object>> describe() {
    Map<String, Map.Entry<String, Object>> properties = new LinkedHashMap<>();

    properties.put(ElasticConstants.HOSTS,
        new AbstractMap.SimpleEntry<>("Comma-separated list of hostnames",
            this.hosts == null ? "" : String.join(",", hosts)));
    properties.put(ElasticConstants.PORT, new AbstractMap.SimpleEntry<>(
        "The port to connect to elastic server.", this.port));
    properties.put(ElasticConstants.INDEX, new AbstractMap.SimpleEntry<>(
        "Default index to send documents to.", this.defaultIndex));
    properties.put(ElasticConstants.USER, new AbstractMap.SimpleEntry<>(
        "Username for auth credentials", this.user));
    properties.put(ElasticConstants.PASSWORD, new AbstractMap.SimpleEntry<>(
        "Password for auth credentials", this.password));
    properties.put(ElasticConstants.MAX_BULK_DOCS,
        new AbstractMap.SimpleEntry<>(
            "Maximum size of the bulk in number of documents.",
            this.maxBulkDocs));
    properties.put(ElasticConstants.MAX_BULK_LENGTH,
        new AbstractMap.SimpleEntry<>("Maximum size of the bulk in bytes.",
            this.maxBulkLength));
    properties.put(ElasticConstants.EXPONENTIAL_BACKOFF_MILLIS,
        new AbstractMap.SimpleEntry<>(
            "Initial delay for the BulkProcessor exponential backoff policy.",
            this.expBackoffMillis));
    properties.put(ElasticConstants.EXPONENTIAL_BACKOFF_RETRIES,
        new AbstractMap.SimpleEntry<>(
            "Number of times the BulkProcessor exponential backoff policy should retry bulk operations.",
            this.expBackoffRetries));
    properties.put(ElasticConstants.BULK_CLOSE_TIMEOUT,
        new AbstractMap.SimpleEntry<>(
            "Number of seconds allowed for the BulkProcessor to complete its last operation.",
            this.bulkCloseTimeout));

    return properties;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
  }

  @Override
  public Configuration getConf() {
    return config;
  }
}