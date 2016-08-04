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

//TODO trust self signed and non matching certs: http://stackoverflow.com/questions/2893819/telling-java-to-accept-self-signed-ssl-certificate
//TODO refactor the dependencies out of root ivy file

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
import org.apache.hadoop.mapred.JobConf;
import org.apache.http.HttpResponse;
import org.apache.http.concurrent.BasicFuture;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.NutchDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


/**
 */
public class ElasticRestIndexWriter implements IndexWriter {
    public static Logger LOG = LoggerFactory.getLogger(ElasticRestIndexWriter.class);

    private static final int DEFAULT_MAX_BULK_DOCS = 250;
    private static final int DEFAULT_MAX_BULK_LENGTH = 2500500;

    private JestClient client;
    private String defaultIndex;
    private String defaultType = null;

    private Configuration config;

    private Bulk.Builder bulkBuilder;
    private Future<HttpResponse> execute;
    private int port = -1;
    private String host = null;
    private String user = null;
    private Boolean https = null;
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

    @Override
    public void open(JobConf job, String name) throws IOException {

        host = job.get(ElasticRestConstants.HOST);
        port = job.getInt(ElasticRestConstants.PORT, 9200);
        user = job.get(ElasticRestConstants.USER);
        password = job.get(ElasticRestConstants.PASSWORD);
        https = job.getBoolean(ElasticRestConstants.HTTPS, false);
        trustAllHostnames = job.getBoolean(ElasticRestConstants.HOSTNAME_TRUST, false);

        // trust ALL certificates
        SSLContext sslContext = null;
        try {
            sslContext = new SSLContextBuilder().loadTrustMaterial(new TrustStrategy() {
                public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
                    return true;
                }
            }).build();
        } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
            e.printStackTrace();
        }

        // skip hostname checks
        HostnameVerifier hostnameVerifier = null;
        if (trustAllHostnames) {
            hostnameVerifier = new NoopHostnameVerifier();
        } else {
            hostnameVerifier = new DefaultHostnameVerifier();
        }

        SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext);
        SchemeIOSessionStrategy httpsIOSessionStrategy = new SSLIOSessionStrategy(sslContext, hostnameVerifier);

        JestClientFactory jestClientFactory = new JestClientFactory();
        URL urlOfElasticsearchNode = new URL(https ? "https" : "http", host, port, "");


        if (host != null && port > 1) {
            HttpClientConfig.Builder builder = new HttpClientConfig.Builder(urlOfElasticsearchNode.toString())
                    .multiThreaded(true)
                    .connTimeout(300000)
                    .readTimeout(300000);
            if (https) {
                if (user != null && password != null) {
                    builder.defaultCredentials(user, password);
                }
                builder.defaultSchemeForDiscoveredNodes("https")
                        .sslSocketFactory(sslSocketFactory) // this only affects sync calls
                        .httpsIOSessionStrategy(httpsIOSessionStrategy); // this only affects async calls

            }
            jestClientFactory.setHttpClientConfig(builder.build());
        } else {
            throw new RuntimeException("No host and port specified");
        }

        client = jestClientFactory.getObject();

        defaultIndex = job.get(ElasticRestConstants.INDEX, "nutch");
        defaultType = job.get(ElasticRestConstants.TYPE, "doc");

        maxBulkDocs = job.getInt(ElasticRestConstants.MAX_BULK_DOCS,
                DEFAULT_MAX_BULK_DOCS);
        maxBulkLength = job.getInt(ElasticRestConstants.MAX_BULK_LENGTH,
                DEFAULT_MAX_BULK_LENGTH);

        bulkBuilder = new Bulk.Builder()
                .defaultIndex(defaultIndex)
                .defaultType(defaultType);

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
            if (doc.getField(fieldName).getValues().size() > 1) {
                source.put(fieldName, doc.getFieldValue(fieldName));
                // Loop through the values to keep track of the size of this
                // document
                for (Object value : doc.getField(fieldName).getValues()) {
                    bulkLength += value.toString().length();
                }
            } else {
                source.put(fieldName, doc.getFieldValue(fieldName));
                bulkLength += doc.getFieldValue(fieldName).toString().length();
            }
        }
        Index indexRequest = new Index.Builder(source)
                .index(defaultIndex)
                .type(type)
                .id(id)
                .build();

        // Add this indexing request to a bulk request
        bulkBuilder.addAction(indexRequest);

        indexedDocs++;
        bulkDocs++;

        if (bulkDocs >= maxBulkDocs || bulkLength >= maxBulkLength) {
            LOG.info("Processing bulk request [docs = " + bulkDocs + ", length = "
                    + bulkLength + ", total docs = " + indexedDocs
                    + ", last doc in bulk = '" + id + "']");
            // Flush the bulk of indexing requests
            createNewBulk = true;
            commit();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        try {
            client.execute(new Delete.Builder(key)
                    .index(defaultIndex)
                    .type(defaultType)
                    .build());
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
                LOG.info("Previous took in ms " + millis
                        + ", including wait " + msWaited);
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
                client.executeAsync(bulkBuilder.build(), new JestResultHandler<BulkResult>() {
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
            bulkBuilder = new Bulk.Builder()
                    .defaultIndex(defaultIndex)
                    .defaultType(defaultType);
            bulkDocs = 0;
            bulkLength = 0;
        }
    }

    @Override
    public void close() throws IOException {
        // Flush pending requests
        LOG.info("Processing remaining requests [docs = " + bulkDocs
                + ", length = " + bulkLength + ", total docs = " + indexedDocs + "]");
        createNewBulk = false;
        commit();
        // flush one more time to finalize the last bulk
        LOG.info("Processing to finalize last execute");
        createNewBulk = false;
        commit();

        // Close
        client.shutdownClient();
    }

    @Override
    public String describe() {
        StringBuffer sb = new StringBuffer("ElasticRestIndexWriter\n");
        sb.append("\t").append(ElasticRestConstants.HOST).append(" : hostname\n");
        sb.append("\t").append(ElasticRestConstants.PORT).append(" : port\n");
        sb.append("\t").append(ElasticRestConstants.INDEX)
                .append(" : elastic index command \n");
        sb.append("\t").append(ElasticRestConstants.MAX_BULK_DOCS)
                .append(" : elastic bulk index doc counts. (default 250) \n");
        sb.append("\t").append(ElasticRestConstants.MAX_BULK_LENGTH)
                .append(" : elastic bulk index length. (default 2500500 ~2.5MB)\n");
        return sb.toString();
    }

    @Override
    public void setConf(Configuration conf) {
        config = conf;
        String host = conf.get(ElasticRestConstants.HOST);

        if (StringUtils.isBlank(host)) {
            String message = "Missing elastic.host. It should be set in nutch-site.xml ";
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