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
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.NutchConfiguration;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for ElasticIndexWriter using Testcontainers.
 */
@Testcontainers(disabledWithoutDocker = true)
public class ElasticIndexWriterIT {

  private static final String ELASTICSEARCH_IMAGE =
      "docker.elastic.co/elasticsearch/elasticsearch:7.10.2";

  @Container
  private static final ElasticsearchContainer elasticsearchContainer =
      new ElasticsearchContainer(ELASTICSEARCH_IMAGE)
          .withEnv("discovery.type", "single-node")
          .withEnv("xpack.security.enabled", "false");

  private ElasticIndexWriter indexWriter;
  private Configuration conf;

  @BeforeEach
  void setUp() throws Exception {
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

  @AfterEach
  void tearDown() throws Exception {
    if (indexWriter != null) {
      try {
        indexWriter.close();
      } catch (Exception e) {
        // Ignore if open() failed and close state is invalid
      }
    }
  }

  @Test
  void testWriteAndCommitDocument() throws Exception {
    NutchDocument doc = new NutchDocument();
    doc.add("id", "test-doc-1");
    doc.add("title", "Test Document");
    doc.add("content", "This is a test document for integration testing.");

    assertDoesNotThrow(() -> indexWriter.write(doc));
    assertDoesNotThrow(() -> indexWriter.commit());
    indexWriter.close();
    indexWriter = null;  // prevent tearDown from closing again

    // Refresh index and verify document via RestHighLevelClient
    try (RestHighLevelClient client = new RestHighLevelClient(
        RestClient.builder(
            new HttpHost(elasticsearchContainer.getHost(),
                elasticsearchContainer.getMappedPort(9200),
                "http")))) {
      GetRequest getRequest = new GetRequest("test-index", "test-doc-1");
      GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
      assertTrue(getResponse.isExists(), "Document should exist in index");
      assertNotNull(getResponse.getSource());
      assertEquals("Test Document", getResponse.getSource().get("title"));
    }
  }

  @Test
  void testDeleteDocument() throws Exception {
    String docId = "test-doc-to-delete";

    // Write a document first
    NutchDocument doc = new NutchDocument();
    doc.add("id", docId);
    doc.add("title", "Document to Delete");
    indexWriter.write(doc);
    indexWriter.commit();

    // Delete the document
    assertDoesNotThrow(() -> indexWriter.delete(docId));
    assertDoesNotThrow(() -> indexWriter.commit());
  }
}
