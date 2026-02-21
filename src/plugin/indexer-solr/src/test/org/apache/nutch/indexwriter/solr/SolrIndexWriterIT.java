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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexerMapReduce;
import org.apache.nutch.indexer.AbstractIndexWriterIT;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.solr.SolrContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for SolrIndexWriter using Testcontainers.
 */
@Testcontainers(disabledWithoutDocker = true)
public class SolrIndexWriterIT extends AbstractIndexWriterIT {

  private static final String SOLR_IMAGE = "solr:8.11.2";
  private static final String COLLECTION = "nutch-test";

  @Container
  private static final SolrContainer solrContainer =
      new SolrContainer(SOLR_IMAGE).withCollection(COLLECTION);

  private SolrIndexWriter indexWriter;
  private Configuration conf;

  @Override
  public void setUpIndexWriter() throws Exception {
    conf = NutchConfiguration.create();
    conf.setBoolean(IndexerMapReduce.INDEXER_DELETE, false);

    indexWriter = new SolrIndexWriter();
    indexWriter.setConf(conf);

    String solrUrl = "http://" + solrContainer.getHost() + ":"
        + solrContainer.getSolrPort() + "/solr/" + COLLECTION;

    Map<String, String> params = new HashMap<>();
    params.put(SolrConstants.SERVER_TYPE, "http");
    params.put(SolrConstants.SERVER_URLS, solrUrl);
    params.put(SolrConstants.COLLECTION, COLLECTION);
    params.put(SolrConstants.COMMIT_SIZE, "100");

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
    try (SolrClient client = new Http2SolrClient.Builder(
        "http://" + solrContainer.getHost() + ":"
            + solrContainer.getSolrPort() + "/solr/" + COLLECTION).build()) {
      ModifiableSolrParams queryParams = new ModifiableSolrParams();
      queryParams.set("q", "id:" + docId);
      QueryResponse response = client.query(queryParams);
      assertTrue(response.getResults().getNumFound() >= 1,
          "Document should exist in Solr");
      Object titleValue = response.getResults().get(0).getFieldValue("title");
      String title = titleValue instanceof Collection
          ? ((Collection<?>) titleValue).iterator().next().toString()
          : titleValue.toString();
      assertEquals(expectedTitle, title);
    }
  }

  @Override
  public IndexWriter prepareWriterForDeleteTest() throws Exception {
    tearDownIndexWriter();

    Configuration deleteConf = NutchConfiguration.create();
    deleteConf.setBoolean(IndexerMapReduce.INDEXER_DELETE, true);
    SolrIndexWriter deleteWriter = new SolrIndexWriter();
    deleteWriter.setConf(deleteConf);

    String solrUrl = "http://" + solrContainer.getHost() + ":"
        + solrContainer.getSolrPort() + "/solr/" + COLLECTION;
    Map<String, String> params = new HashMap<>();
    params.put(SolrConstants.SERVER_TYPE, "http");
    params.put(SolrConstants.SERVER_URLS, solrUrl);
    params.put(SolrConstants.COLLECTION, COLLECTION);
    deleteWriter.open(new IndexWriterParams(params));

    return deleteWriter;
  }
}
