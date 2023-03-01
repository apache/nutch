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
package org.apache.nutch.indexwriter.opensearch1x;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.util.StringUtil;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.format.DateTimeFormatter;

import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Sends NutchDocuments to a configured OpenSearch index.
 */
public class OpenSearch1xIndexWriter implements IndexWriter {
  private static final Logger LOG = LoggerFactory.getLogger(
      MethodHandles.lookup().lookupClass());

  private static final int DEFAULT_PORT = 9300;
  private static final int DEFAULT_MAX_BULK_DOCS = 250;
  private static final int DEFAULT_MAX_BULK_LENGTH = 2500500;
  private static final int DEFAULT_EXP_BACKOFF_MILLIS = 100;
  private static final int DEFAULT_EXP_BACKOFF_RETRIES = 10;
  private static final int DEFAULT_BULK_CLOSE_TIMEOUT = 600;
  private static final String DEFAULT_INDEX = "nutch";
  private static final String DEFAULT_USER = "admin";
  private static final String DEFAULT_PASSWORD = "admin";
  private String[] hosts;
  private int port;
  private String scheme = "https";
  private String user;
  private String password;
  private String trustStorePath;
  private String trustStorePassword;
  private String trustStoreType;
  private String keyStorePath;
  private String keyStorePassword;
  private String keyStoreType;
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
   * Initializes the internal variables from a given index writer
   * configuration.
   *
   * @param parameters
   *     Params from the index writer configuration.
   * @throws IOException
   *     Some exception thrown by writer.
   */
  @Override
  public void open(IndexWriterParams parameters) throws IOException {

    String hosts = parameters.get(OpenSearch1xConstants.HOSTS);

    if (StringUtils.isBlank(hosts)) {
      String message = "Missing " + OpenSearch1xConstants.HOSTS + ". this should be set in index-writers.xml ";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    bulkCloseTimeout = parameters.getLong(
        OpenSearch1xConstants.BULK_CLOSE_TIMEOUT, DEFAULT_BULK_CLOSE_TIMEOUT);
    defaultIndex = parameters.get(OpenSearch1xConstants.INDEX, DEFAULT_INDEX);

    maxBulkDocs = parameters.getInt(OpenSearch1xConstants.MAX_BULK_DOCS,
        DEFAULT_MAX_BULK_DOCS);
    maxBulkLength = parameters.getInt(OpenSearch1xConstants.MAX_BULK_LENGTH,
        DEFAULT_MAX_BULK_LENGTH);
    expBackoffMillis = parameters.getInt(
        OpenSearch1xConstants.EXPONENTIAL_BACKOFF_MILLIS,
        DEFAULT_EXP_BACKOFF_MILLIS);
    expBackoffRetries = parameters.getInt(
        OpenSearch1xConstants.EXPONENTIAL_BACKOFF_RETRIES,
        DEFAULT_EXP_BACKOFF_RETRIES);

    client = makeClient(parameters);

    LOG.debug("Creating BulkProcessor with maxBulkDocs={}, maxBulkLength={}",
        maxBulkDocs, maxBulkLength);

    bulkProcessor = BulkProcessor.builder(
            (request, bulkListener) -> client.bulkAsync(request,
                RequestOptions.DEFAULT, bulkListener), bulkProcessorListener())
        .setBulkActions(maxBulkDocs)
        .setBulkSize(new ByteSizeValue(maxBulkLength, ByteSizeUnit.BYTES))
        .setConcurrentRequests(1).setBackoffPolicy(
            BackoffPolicy.exponentialBackoff(
                TimeValue.timeValueMillis(expBackoffMillis), expBackoffRetries))
        .build();
  }

  /**
   * Generates a RestHighLevelClient with the hosts given
   *
   * @param parameters
   *     implementation specific
   *     {@link org.apache.nutch.indexer.IndexWriterParams}
   * @return an initialized {@link org.opensearch.client.RestHighLevelClient}
   * @throws IOException
   *     if there is an error reading the
   *     {@link org.apache.nutch.indexer.IndexWriterParams}
   */
  protected RestHighLevelClient makeClient(IndexWriterParams parameters)
      throws IOException {
    hosts = parameters.getStrings(OpenSearch1xConstants.HOSTS);
    port = parameters.getInt(OpenSearch1xConstants.PORT, DEFAULT_PORT);
    scheme = parameters.get(OpenSearch1xConstants.SCHEME,
        HttpHost.DEFAULT_SCHEME_NAME);
    user = parameters.get(OpenSearch1xConstants.USER, DEFAULT_USER);
    password = parameters.get(OpenSearch1xConstants.PASSWORD, DEFAULT_PASSWORD);

    trustStorePath = parameters.get(OpenSearch1xConstants.TRUST_STORE_PATH);
    trustStorePassword = parameters.get(OpenSearch1xConstants.TRUST_STORE_PASSWORD);
    trustStoreType = parameters.get(OpenSearch1xConstants.TRUST_STORE_TYPE, "JKS");

    keyStorePath = parameters.get(OpenSearch1xConstants.KEY_STORE_PATH);
    keyStorePassword = parameters.get(OpenSearch1xConstants.KEY_STORE_PASSWORD);
    keyStoreType = parameters.get(OpenSearch1xConstants.KEY_STORE_TYPE, "JKS");
    boolean basicAuth = user != null && password != null;

    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    if (basicAuth) {
      credentialsProvider.setCredentials(AuthScope.ANY,
          new UsernamePasswordCredentials(user, password));
    }

    RestHighLevelClient client = null;

    if (hosts != null && port > 1) {
      HttpHost[] hostsList = new HttpHost[hosts.length];
      int i = 0;
      for (String host : hosts) {
        hostsList[i++] = new HttpHost(host, port, scheme);
      }
      RestClientBuilder restClientBuilder = RestClient.builder(hostsList);

      if ("http".equals(scheme) && basicAuth) {
        restClientBuilder.setHttpClientConfigCallback(
            new HttpClientConfigCallback() {
              @Override
              public HttpAsyncClientBuilder customizeHttpClient(
                  HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(
                    credentialsProvider);
              }
            });
      } else if ("https".equals(scheme)) {
        try {

          final SSLContext sslContext = createSSLContext();
          restClientBuilder.setHttpClientConfigCallback(
              new HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(
                    HttpAsyncClientBuilder httpClientBuilder) {
                  if (basicAuth) {
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                  }
                  return httpClientBuilder.setSSLContext(sslContext);
                }
              });
        } catch (Exception e) {
          LOG.error("Error setting up SSLContext because: " + e.getMessage(),
              e);
        }
      }

      client = new RestHighLevelClient(restClientBuilder);
    } else {
      throw new IOException(
          "OpenSearchRestClient initialization Failed!!!\\n\\nPlease Provide the hosts");
    }

    return client;
  }

  private SSLContext createSSLContext() throws GeneralSecurityException, IOException {

    SSLContextBuilder sslBuilder = SSLContexts.custom();
    Optional<KeyStore> trustStore = loadStore(trustStorePath, trustStorePassword, trustStoreType);
    Optional<KeyStore> keyStore = loadStore(keyStorePath, keyStorePassword, keyStoreType);

    if (trustStore.isPresent()) {
      sslBuilder.loadTrustMaterial(trustStore.get(), null);
    } else {
      LOG.warn("You haven't set up a trust store. We're effectively turning off " +
          " tls.  This is 'Not a good idea'(tm). See a getting started guide: " +
          "https://opensearch.org/blog/connecting-java-high-level-rest-client-with-opensearch-over-https/ "+
          " or in more depth: https://opensearch.org/docs/latest/security/configuration/tls/");
      sslBuilder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
    }

    if (keyStore.isPresent()) {
      //assuming the keystore and the key have the same password
      sslBuilder.loadKeyMaterial(keyStore.get(), keyStorePassword.toCharArray());
    }
    return sslBuilder.build();
  }

  private Optional<KeyStore> loadStore(String storePath, String storePassword, String storeType)
      throws GeneralSecurityException, IOException {
    if (StringUtils.isAllBlank(storePath)) {
      return Optional.empty();
    }
    if (storePassword == null) {
      throw new IllegalArgumentException("must include a non-null password for store: " + storePath);
    }

    KeyStore store = KeyStore.getInstance(storeType);
    try (InputStream is = Files.newInputStream(
        Paths.get(storePath))) {
      store.load(is, storePassword.toCharArray());
    }
    return Optional.of(store);
  }

  /**
   * Generates a default BulkProcessor.Listener
   *
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
        LOG.error("Opensearch indexing failed:", failure);
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
          value = DateTimeFormatter.ISO_INSTANT.format(
              ((java.util.Date) value).toInstant());
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
   *     &#60;KEY,&#60;DESCRIPTION,VALUE&#62;&#62;.
   */
  @Override
  public Map<String, Map.Entry<String, Object>> describe() {
    Map<String, Map.Entry<String, Object>> properties = new LinkedHashMap<>();

    properties.put(OpenSearch1xConstants.HOSTS,
        new AbstractMap.SimpleEntry<>("Comma-separated list of hostnames",
            this.hosts == null ? "" : String.join(",", hosts)));
    properties.put(OpenSearch1xConstants.PORT,
        new AbstractMap.SimpleEntry<>("The port to connect to opensearch server.",
            this.port));
    properties.put(OpenSearch1xConstants.SCHEME, new AbstractMap.SimpleEntry<>(
        "The scheme (http or https) to connect to opensearch server.",
        this.scheme));
    properties.put(OpenSearch1xConstants.INDEX,
        new AbstractMap.SimpleEntry<>("Default index to send documents to.",
            this.defaultIndex));
    properties.put(OpenSearch1xConstants.USER,
        new AbstractMap.SimpleEntry<>("Username for auth credentials",
            this.user));
    properties.put(OpenSearch1xConstants.PASSWORD,
        new AbstractMap.SimpleEntry<>("Password for auth credentials",
            StringUtil.mask(getOrEmptyString(this.password))));

    properties.put(OpenSearch1xConstants.TRUST_STORE_PATH,
        new AbstractMap.SimpleEntry<>("Trust store path", this.trustStorePath));
    properties.put(OpenSearch1xConstants.TRUST_STORE_PASSWORD,
        new AbstractMap.SimpleEntry<>("Password for trust store",
            StringUtil.mask(getOrEmptyString(this.trustStorePassword))));
    properties.put(OpenSearch1xConstants.TRUST_STORE_TYPE,
        new AbstractMap.SimpleEntry<>("Trust store type (default=JKS)",
            this.trustStoreType));

    properties.put(OpenSearch1xConstants.KEY_STORE_PATH,
        new AbstractMap.SimpleEntry<>("Key store path", this.keyStorePath));
    properties.put(OpenSearch1xConstants.KEY_STORE_PASSWORD,
        new AbstractMap.SimpleEntry<>("Password for key and key store",
            StringUtil.mask(getOrEmptyString(this.keyStorePassword))));
    properties.put(OpenSearch1xConstants.KEY_STORE_TYPE,
        new AbstractMap.SimpleEntry<>("Key store type (default=JKS)",
            this.keyStoreType));

    properties.put(OpenSearch1xConstants.MAX_BULK_DOCS,
        new AbstractMap.SimpleEntry<>(
            "Maximum size of the bulk in number of documents.",
            this.maxBulkDocs));
    properties.put(OpenSearch1xConstants.MAX_BULK_LENGTH,
        new AbstractMap.SimpleEntry<>("Maximum size of the bulk in bytes.",
            this.maxBulkLength));
    properties.put(OpenSearch1xConstants.EXPONENTIAL_BACKOFF_MILLIS,
        new AbstractMap.SimpleEntry<>(
            "Initial delay for the BulkProcessor exponential backoff policy.",
            this.expBackoffMillis));
    properties.put(OpenSearch1xConstants.EXPONENTIAL_BACKOFF_RETRIES,
        new AbstractMap.SimpleEntry<>(
            "Number of times the BulkProcessor exponential backoff policy should retry bulk operations.",
            this.expBackoffRetries));
    properties.put(OpenSearch1xConstants.BULK_CLOSE_TIMEOUT,
        new AbstractMap.SimpleEntry<>(
            "Number of seconds allowed for the BulkProcessor to complete its last operation.",
            this.bulkCloseTimeout));

    return properties;
  }

  private static String getOrEmptyString(String nullable) {
    return nullable != null ? nullable : StringUtils.EMPTY;
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
