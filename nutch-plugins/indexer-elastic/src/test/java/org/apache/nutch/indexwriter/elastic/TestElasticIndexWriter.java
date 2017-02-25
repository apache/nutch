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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.NutchConfiguration;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestElasticIndexWriter {

  boolean bulkRequestSuccessful, clusterSaturated;
  int curNumFailures, maxNumFailures;
  Configuration conf;
  Client client;
  ElasticIndexWriter testIndexWriter;

  @Before
  public void setup() {
    conf = NutchConfiguration.create();
    conf.addResource("nutch-site-test.xml");

    bulkRequestSuccessful = false;
    clusterSaturated = false;
    curNumFailures = 0;
    maxNumFailures = 0;

    Settings settings = Settings.builder().build();
    ThreadPool threadPool = new ThreadPool(settings);
    Headers headers = new Headers(settings);

    // customize the ES client to simulate responses from an ES cluster
    client = new AbstractClient(settings, threadPool, headers) {
      @Override
      public void close() { }

      @Override
      protected <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
          Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {

        BulkResponse response = null;
        if (clusterSaturated) {
          // pretend the cluster is saturated
          curNumFailures++;
          if (curNumFailures >= maxNumFailures) {
            // pretend the cluster is suddenly no longer saturated
            clusterSaturated = false;
          }

          // respond with a failure
          BulkItemResponse failed = new BulkItemResponse(0, "index",
              new BulkItemResponse.Failure("nutch", "index", "failure0",
                  new EsRejectedExecutionException("saturated")));
          response = new BulkResponse(new BulkItemResponse[]{failed}, 0);
        } else {
          // respond successfully
          BulkItemResponse success = new BulkItemResponse(0, "index",
              new IndexResponse("nutch", "index", "index0", 0, true));
          response = new BulkResponse(new BulkItemResponse[]{success}, 0);
        }

        listener.onResponse((Response)response);
      }
    };

    // customize the plugin to signal successful bulk operations
    testIndexWriter = new ElasticIndexWriter() {
      @Override
      protected Client makeClient(Configuration conf) {
        return client;
      }

      @Override
      protected BulkProcessor.Listener bulkProcessorListener() {
        return new BulkProcessor.Listener() {

          @Override
          public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (!response.hasFailures()) {
              bulkRequestSuccessful = true;
            }
          }

          @Override
          public void afterBulk(long executionId, BulkRequest request, Throwable failure) { }

          @Override
          public void beforeBulk(long executionId, BulkRequest request) { }
        };
      }
    };
  }

  @Test
  public void testBulkMaxDocs() throws IOException {
    int numDocs = 10;
    conf.setInt(ElasticConstants.MAX_BULK_DOCS, numDocs);
    JobConf job = new JobConf(conf);

    testIndexWriter.setConf(conf);
    testIndexWriter.open(job, "name");

    NutchDocument doc = new NutchDocument();
    doc.add("id", "http://www.example.com");

    Assert.assertFalse(bulkRequestSuccessful);

    for (int i = 0; i < numDocs; i++) {
      testIndexWriter.write(doc);
    }

    testIndexWriter.close();

    Assert.assertTrue(bulkRequestSuccessful);
  }

  @Test
  public void testBulkMaxLength() throws IOException {
    String key = "id";
    String value = "http://www.example.com";

    int defaultMaxBulkLength = conf.getInt(ElasticConstants.MAX_BULK_LENGTH, 2500500);

    // Test that MAX_BULK_LENGTH is respected by lowering it 10x
    int testMaxBulkLength = defaultMaxBulkLength / 10;

    // This number is somewhat arbitrary, but must be a function of:
    // - testMaxBulkLength
    // - approximate size of each doc
    int numDocs = testMaxBulkLength / (key.length() + value.length());

    conf.setInt(ElasticConstants.MAX_BULK_LENGTH, testMaxBulkLength);
    JobConf job = new JobConf(conf);

    testIndexWriter.setConf(conf);
    testIndexWriter.open(job, "name");

    NutchDocument doc = new NutchDocument();
    doc.add(key, value);

    Assert.assertFalse(bulkRequestSuccessful);

    for (int i = 0; i < numDocs; i++) {
      testIndexWriter.write(doc);
    }

    testIndexWriter.close();

    Assert.assertTrue(bulkRequestSuccessful);
  }

  @Test
  public void testBackoffPolicy() throws IOException {
    // set a non-zero "max-retry" value, **implying the cluster is saturated**
    maxNumFailures = 5;
    conf.setInt(ElasticConstants.EXPONENTIAL_BACKOFF_RETRIES, maxNumFailures);

    int numDocs = 10;
    conf.setInt(ElasticConstants.MAX_BULK_DOCS, numDocs);

    JobConf job = new JobConf(conf);

    testIndexWriter.setConf(conf);
    testIndexWriter.open(job, "name");

    NutchDocument doc = new NutchDocument();
    doc.add("id", "http://www.example.com");

    // pretend the remote cluster is "saturated"
    clusterSaturated = true;

    Assert.assertFalse(bulkRequestSuccessful);

    // write enough docs to initiate one bulk request
    for (int i = 0; i < numDocs; i++) {
      testIndexWriter.write(doc);
    }

    testIndexWriter.close();

    // the BulkProcessor should have retried `maxNumFailures + 1` times, then succeeded
    Assert.assertTrue(bulkRequestSuccessful);
  }

}
