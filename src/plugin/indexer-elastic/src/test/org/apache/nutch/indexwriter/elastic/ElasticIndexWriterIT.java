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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.HttpHost;
import org.apache.nutch.indexer.AbstractIndexWriterIT;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.NutchConfiguration;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for ElasticIndexWriter using Testcontainers.
 */
@Testcontainers(disabledWithoutDocker = true)
public class ElasticIndexWriterIT extends AbstractIndexWriterIT {

  private static final String ELASTICSEARCH_IMAGE =
      "docker.elastic.co/elasticsearch/elasticsearch:7.10.2";

  @Container
  private static final ElasticsearchContainer elasticsearchContainer =
      new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
          .withEnv("discovery.type", "single-node")
          .withEnv("xpack.security.enabled", "false");

  private ElasticIndexWriter indexWriter;
  private Configuration conf;

  @Override
  public void setUpIndexWriter() throws Exception {
    conf = NutchConfiguration.create();
    indexWriter = new ElasticIndexWriter();
    indexWriter.setConf(conf);

    Map<String, String> params = new HashMap<>();
    params.put(ElasticConstants.HOSTS, elasticsearchContainer.getHost());
    params.put(ElasticConstants.PORT, String.valueOf(elasticsearchContainer.getMappedPort(9200)));
    params.put(ElasticConstants.INDEX, "test-index");
    params.put(ElasticConstants.SCHEME, "http");

    IndexWriterParams writerParams = new IndexWriterParams(params);
    indexWriter.open(writerParams);
  }

  @Override
  public void tearDownIndexWriter() throws Exception {
    if (indexWriter != null) {
      try {
        indexWriter.close();
      } catch (Exception e) {
        // Ignore if open() failed and close state is invalid
      }
      indexWriter = null;
    }
  }

  @Override
  public IndexWriter getIndexWriter() {
    return indexWriter;
  }

  @Override
  public boolean supportsDelete() {
    return true;
  }

  @Override
  public void verifyDocumentWritten(String docId, String expectedTitle) throws Exception {
    try (RestHighLevelClient client = new RestHighLevelClient(
        RestClient.builder(
            new HttpHost(elasticsearchContainer.getHost(),
                elasticsearchContainer.getMappedPort(9200),
                "http")))) {
      GetRequest getRequest = new GetRequest("test-index", docId);
      GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
      assertTrue(getResponse.isExists(), "Document should exist in index");
      assertNotNull(getResponse.getSource());
      assertEquals(expectedTitle, getResponse.getSource().get("title"));
    }
  }
}
