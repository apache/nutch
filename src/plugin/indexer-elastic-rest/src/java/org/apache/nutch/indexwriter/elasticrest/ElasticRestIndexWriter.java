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
package org.apache.nutch.indexwriter.elasticrest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.concurrent.BasicFuture;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.AbstractMap;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 */
public class ElasticRestIndexWriter implements IndexWriter {
  public static Logger LOG = LoggerFactory
      .getLogger(ElasticRestIndexWriter.class);

  private static final int DEFAULT_MAX_BULK_DOCS = 250;
  private static final int DEFAULT_MAX_BULK_LENGTH = 2500500;
  private static final String DEFAULT_SEPARATOR = "_";
  private static final String DEFAULT_SINK = "others";

  private JestClient client;
  private String defaultIndex;
  private String defaultType = null;

  private Configuration config;

  private Bulk.Builder bulkBuilder;
  private int port = -1;
  private String host = null;
  private Boolean https = null;
  private String user = null;
  private String password = null;
  private Boolean trustAllHostnames = null;

  private int maxBulkDocs;
  private int maxBulkLength;
  private long indexedDocs = 0;
  private int bulkDocs = 0;
  private int bulkLength = 0;
  private boolean createNewBulk = false;
  private long millis;
  private BasicFuture<JestResult> basicFuture = null;

  private String[] languages = null;
  private String separator = null;
  private String sink = null;

  @Override
  public void open(Configuration conf, String name) throws IOException {
    //Implementation not required
  }

  @Override
  public void open(IndexWriterParams parameters) throws IOException {
    host = parameters.get(ElasticRestConstants.HOST);
    if (StringUtils.isBlank(host)) {
      String message = "Missing host. It should be set in index-writers.xml";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }

    port = parameters.getInt(ElasticRestConstants.PORT, 9200);
    user = parameters.get(ElasticRestConstants.USER);
    password = parameters.get(ElasticRestConstants.PASSWORD);
    https = parameters.getBoolean(ElasticRestConstants.HTTPS, false);
    trustAllHostnames = parameters
        .getBoolean(ElasticRestConstants.HOSTNAME_TRUST, false);

    languages = parameters.getStrings(ElasticRestConstants.LANGUAGES);
    separator = parameters
        .get(ElasticRestConstants.SEPARATOR, DEFAULT_SEPARATOR);
    sink = parameters.get(ElasticRestConstants.SINK, DEFAULT_SINK);

    // trust ALL certificates
    SSLContext sslContext = null;
    try {
      sslContext = new SSLContextBuilder()
          .loadTrustMaterial(new TrustStrategy() {
            public boolean isTrusted(X509Certificate[] arg0, String arg1)
                throws CertificateException {
              return true;
            }
          }).build();
    } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
      LOG.error("Failed to instantiate sslcontext object: \n{}",
          ExceptionUtils.getStackTrace(e));
      throw new SecurityException();
    }

    // skip hostname checks
    HostnameVerifier hostnameVerifier = null;
    if (trustAllHostnames) {
      hostnameVerifier = NoopHostnameVerifier.INSTANCE;
    } else {
      hostnameVerifier = new DefaultHostnameVerifier();
    }

    SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(
        sslContext);
    SchemeIOSessionStrategy httpsIOSessionStrategy = new SSLIOSessionStrategy(
        sslContext, hostnameVerifier);

    JestClientFactory jestClientFactory = new JestClientFactory();
    URL urlOfElasticsearchNode = new URL(https ? "https" : "http", host, port,
        "");

    if (host != null && port > 1) {
      HttpClientConfig.Builder builder = new HttpClientConfig.Builder(
          urlOfElasticsearchNode.toString()).multiThreaded(true)
          .connTimeout(300000).readTimeout(300000);
      if (https) {
        if (user != null && password != null) {
          builder.defaultCredentials(user, password);
        }
        builder.defaultSchemeForDiscoveredNodes("https")
            .sslSocketFactory(sslSocketFactory) // this only affects sync calls
            .httpsIOSessionStrategy(
                httpsIOSessionStrategy); // this only affects async calls
      }
      jestClientFactory.setHttpClientConfig(builder.build());
    } else {
      throw new IllegalStateException(
          "No host or port specified. Please set the host and port in nutch-site.xml");
    }

    client = jestClientFactory.getObject();

    defaultIndex = parameters.get(ElasticRestConstants.INDEX, "nutch");
    defaultType = parameters.get(ElasticRestConstants.TYPE, "doc");

    maxBulkDocs = parameters
        .getInt(ElasticRestConstants.MAX_BULK_DOCS, DEFAULT_MAX_BULK_DOCS);
    maxBulkLength = parameters
        .getInt(ElasticRestConstants.MAX_BULK_LENGTH, DEFAULT_MAX_BULK_LENGTH);

    bulkBuilder = new Bulk.Builder().defaultIndex(defaultIndex)
        .defaultType(defaultType);
  }

  private static Object normalizeValue(Object value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Map || value instanceof Date) {
      return value;
    }

    return value.toString();
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    String id = (String) doc.getFieldValue("id");
    String type = doc.getDocumentMeta().get("type");
    if (type == null) {
      type = defaultType;
    }

    Map<String, Object> source = new HashMap<String, Object>();

    // Loop through all fields of this doc
    for (String fieldName : doc.getFieldNames()) {
      Set<Object> allFieldValues = new LinkedHashSet<>(
          doc.getField(fieldName).getValues());

      if (allFieldValues.size() > 1) {
        Object[] normalizedFieldValues = allFieldValues.stream()
            .map(ElasticRestIndexWriter::normalizeValue).toArray();

        // Loop through the values to keep track of the size of this document
        for (Object value : normalizedFieldValues) {
          bulkLength += value.toString().length();
        }

        source.put(fieldName, normalizedFieldValues);
      } else if (allFieldValues.size() == 1) {
        Object normalizedFieldValue = normalizeValue(
            allFieldValues.iterator().next());
        source.put(fieldName, normalizedFieldValue);
        bulkLength += normalizedFieldValue.toString().length();
      }
    }

    String index;
    if (languages != null && languages.length > 0) {
      String language = (String) doc.getFieldValue("lang");
      boolean exists = false;
      for (String lang : languages) {
        if (lang.equals(language)) {
          exists = true;
          break;
        }
      }
      if (exists) {
        index = getLanguageIndexName(language);
      } else {
        index = getSinkIndexName();
      }
    } else {
      index = defaultIndex;
    }
    Index indexRequest = new Index.Builder(source).index(index).type(type)
        .id(id).build();

    // Add this indexing request to a bulk request
    bulkBuilder.addAction(indexRequest);

    indexedDocs++;
    bulkDocs++;

    if (bulkDocs >= maxBulkDocs || bulkLength >= maxBulkLength) {
      LOG.info(
          "Processing bulk request [docs = {}, length = {}, total docs = {}, last doc in bulk = '{}']",
          bulkDocs, bulkLength, indexedDocs, id);
      // Flush the bulk of indexing requests
      createNewBulk = true;
      commit();
    }
  }

  @Override
  public void delete(String key) throws IOException {
    try {
      if (languages != null && languages.length > 0) {
        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultType(defaultType);
        for (String lang : languages) {
          bulkBuilder.addAction(
              new Delete.Builder(key).index(getLanguageIndexName(lang))
                  .type(defaultType).build());
        }
        bulkBuilder.addAction(
            new Delete.Builder(key).index(getSinkIndexName()).type(defaultType)
                .build());
        client.execute(bulkBuilder.build());
      } else {
        client.execute(
            new Delete.Builder(key).index(defaultIndex).type(defaultType)
                .build());
      }
    } catch (IOException e) {
      LOG.error(ExceptionUtils.getStackTrace(e));
      throw e;
    }
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    try {
      write(doc);
    } catch (IOException e) {
      LOG.error(ExceptionUtils.getStackTrace(e));
      throw e;
    }
  }

  @Override
  public void commit() throws IOException {
    if (basicFuture != null) {
      // wait for previous to finish
      long beforeWait = System.currentTimeMillis();
      try {
        JestResult result = basicFuture.get();
        if (result == null) {
          throw new RuntimeException();
        }
        long msWaited = System.currentTimeMillis() - beforeWait;
        LOG.info("Previous took in ms {}, including wait {}", millis, msWaited);
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error waiting for result ", e);
      }
      basicFuture = null;
    }
    if (bulkBuilder != null) {
      if (bulkDocs > 0) {
        // start a flush, note that this is an asynchronous call
        basicFuture = new BasicFuture<>(null);
        millis = System.currentTimeMillis();
        client.executeAsync(bulkBuilder.build(),
            new JestResultHandler<BulkResult>() {
              @Override
              public void completed(BulkResult bulkResult) {
                basicFuture.completed(bulkResult);
                millis = System.currentTimeMillis() - millis;
              }

              @Override
              public void failed(Exception e) {
                basicFuture.completed(null);
                LOG.error("Failed result: ", e);
              }
            });
      }
      bulkBuilder = null;
    }
    if (createNewBulk) {
      // Prepare a new bulk request
      bulkBuilder = new Bulk.Builder().defaultIndex(defaultIndex)
          .defaultType(defaultType);
      bulkDocs = 0;
      bulkLength = 0;
    }
  }

  @Override
  public void close() throws IOException {
    // Flush pending requests
    LOG.info(
        "Processing remaining requests [docs = {}, length = {}, total docs = {}]",
        bulkDocs, bulkLength, indexedDocs);
    createNewBulk = false;
    commit();

    // flush one more time to finalize the last bulk
    LOG.info("Processing to finalize last execute");
    createNewBulk = false;
    commit();

    // Close
    client.shutdownClient();
  }

  /**
   * Returns {@link Map} with the specific parameters the IndexWriter instance can take.
   *
   * @return The values of each row. It must have the form <KEY,<DESCRIPTION,VALUE>>.
   */
  @Override
  public Map<String, Map.Entry<String, Object>> describe() {
    Map<String, Map.Entry<String, Object>> properties = new LinkedHashMap<>();

    properties.put(ElasticRestConstants.HOST, new AbstractMap.SimpleEntry<>(
        "The hostname or a list of comma separated hostnames to send documents "
            + "to using Elasticsearch Jest. Both host and port must be defined.",
        this.host));
    properties.put(ElasticRestConstants.PORT, new AbstractMap.SimpleEntry<>(
        "The port to connect to using Elasticsearch Jest.", this.port));
    properties.put(ElasticRestConstants.INDEX,
        new AbstractMap.SimpleEntry<>("Default index to send documents to.",
            this.defaultIndex));
    properties.put(ElasticRestConstants.MAX_BULK_DOCS,
        new AbstractMap.SimpleEntry<>(
            "Maximum size of the bulk in number of documents.",
            this.maxBulkDocs));
    properties.put(ElasticRestConstants.MAX_BULK_LENGTH,
        new AbstractMap.SimpleEntry<>("Maximum size of the bulk in bytes.",
            this.maxBulkLength));

    properties.put(ElasticRestConstants.USER, new AbstractMap.SimpleEntry<>(
        "Username for auth credentials (only used when https is enabled)",
        this.user));
    properties.put(ElasticRestConstants.PASSWORD, new AbstractMap.SimpleEntry<>(
        "Password for auth credentials (only used when https is enabled)",
        this.password));
    properties.put(ElasticRestConstants.TYPE,
        new AbstractMap.SimpleEntry<>("Default type to send documents to.",
            this.defaultType));
    properties.put(ElasticRestConstants.HTTPS, new AbstractMap.SimpleEntry<>(
        "true to enable https, false to disable https. If you've disabled http "
            + "access (by forcing https), be sure to set this to true, otherwise "
            + "you might get \"connection reset by peer\".", this.https));
    properties.put(ElasticRestConstants.HOSTNAME_TRUST,
        new AbstractMap.SimpleEntry<>(
            "true to trust elasticsearch server's certificate even if its listed "
                + "domain name does not match the domain they are hosted or false "
                + "to check if the elasticsearch server's certificate's listed "
                + "domain is the same domain that it is hosted on, and if "
                + "it doesn't, then fail to index (only used when https is enabled)",
            this.trustAllHostnames));

    properties.put(ElasticRestConstants.LANGUAGES,
        new AbstractMap.SimpleEntry<>(
            "A list of strings denoting the supported languages (e.g. en, de, fr, it). "
                + "If this value is empty all documents will be sent to index property. "
                + "If not empty the Rest client will distribute documents in different "
                + "indices based on their languages property. Indices are named with the "
                + "following schema: index separator language (e.g. nutch_de). "
                + "Entries with an unsupported languages value will be added to "
                + "index index separator sink (e.g. nutch_others).",
            this.languages == null ? "" : String.join(",", languages)));
    properties.put(ElasticRestConstants.SEPARATOR,
        new AbstractMap.SimpleEntry<>(
            "Is used only if languages property is defined to build the index name "
                + "(i.e. index separator lang).", this.separator));
    properties.put(ElasticRestConstants.SINK, new AbstractMap.SimpleEntry<>(
        "Is used only if languages property is defined to build the index name "
            + "where to store documents with unsupported languages "
            + "(i.e. index separator sink).", this.sink));

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

  private String getLanguageIndexName(String lang) {
    return getComposedIndexName(defaultIndex, lang);
  }

  private String getSinkIndexName() {
    return getComposedIndexName(defaultIndex, sink);
  }

  private String getComposedIndexName(String prefix, String postfix) {
    return prefix + separator + postfix;
  }
}
