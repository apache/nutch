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
package org.apache.nutch.indexer.elastic;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchIndexWriter;
import org.apache.nutch.util.TableUtil;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticWriter implements NutchIndexWriter {

  public static Logger LOG = LoggerFactory.getLogger(ElasticWriter.class);

  private static final int DEFAULT_MAX_BULK_DOCS = 500;
  private static final int DEFAULT_MAX_BULK_LENGTH = 5001001; // ~5MB

  private Client client;
  private Node node;
  private String defaultIndex;

  private BulkRequestBuilder bulk;
  private ListenableActionFuture<BulkResponse> execute;
  private int maxBulkDocs;
  private int maxBulkLength;
  private long indexedDocs = 0;
  private int bulkDocs = 0;
  private int bulkLength = 0;

  @Override
  public void write(NutchDocument doc) throws IOException {
    String id = TableUtil.reverseUrl(doc.getFieldValue("url"));
    String type = doc.getDocumentMeta().get("type");
    if (type == null) type = "doc";
    IndexRequestBuilder request = client.prepareIndex(defaultIndex, type, id);
    
    Map<String, Object> source = new HashMap<String, Object>();
    
    // Loop through all fields of this doc
    for (String fieldName : doc.getFieldNames()) {
      if (doc.getFieldValues(fieldName).size() > 1) {
        source.put(fieldName, doc.getFieldValues(fieldName));
        // Loop through the values to keep track of the size of this document
        for (String value : doc.getFieldValues(fieldName)) {
          bulkLength += value.length();
        }
      } else {
        source.put(fieldName, doc.getFieldValue(fieldName));
        bulkLength += doc.getFieldValue(fieldName).length();
      }
    }
    request.setSource(source);
    
    // Add this indexing request to a bulk request
    bulk.add(request);
    indexedDocs++;
    bulkDocs++;
    
    if (bulkDocs >= maxBulkDocs || bulkLength >= maxBulkLength) {
      LOG.info("Processing bulk request [docs = " + bulkDocs + ", length = "
          + bulkLength + ", total docs = " + indexedDocs
          + ", last doc in bulk = '" + id + "']");
      // Flush the bulk of indexing requests
      processExecute(true);
      
    }
  }

  private void processExecute(boolean createNewBulk) {
    if (execute != null) {
      // wait for previous to finish
      long beforeWait = System.currentTimeMillis();
      BulkResponse actionGet = execute.actionGet();
      if (actionGet.hasFailures()) {
        for (BulkItemResponse item : actionGet) {
          if (item.failed()) {
            throw new RuntimeException("First failure in bulk: "
                + item.getFailureMessage());
          }
        }
      }
      long msWaited = System.currentTimeMillis() - beforeWait;
      LOG.info("Previous took in ms " + actionGet.getTookInMillis()
          + ", including wait " + msWaited);
      execute = null;
    }
    if (bulk != null) {
      if (bulkDocs > 0) {
        // start a flush, note that this is an asynchronous call
        execute = bulk.execute();
      }
      bulk = null;
    }
    if (createNewBulk) {
      // Prepare a new bulk request
      bulk = client.prepareBulk();
      bulkDocs = 0;
      bulkLength = 0;
    }
  }

  @Override
  public void close() throws IOException {
    // Flush pending requests
    LOG.info("Processing remaining requests [docs = " + bulkDocs
        + ", length = " + bulkLength + ", total docs = " + indexedDocs + "]");
    processExecute(false);
    // flush one more time to finalize the last bulk
    LOG.info("Processing to finalize last execute");
    processExecute(false);
    
    // Close
    client.close();
    node.close();
  }

  @Override
  public void open(TaskAttemptContext job) throws IOException {
    String clusterName = job.getConfiguration().get(ElasticConstants.CLUSTER);
    if (clusterName != null) {
      node = nodeBuilder().clusterName(clusterName).client(true).node();
    } else {
      node = nodeBuilder().client(true).node();
    }
    client = node.client();
    
    bulk = client.prepareBulk();
    defaultIndex = job.getConfiguration().get(ElasticConstants.INDEX, "index");
    maxBulkDocs = job.getConfiguration().getInt(
        ElasticConstants.MAX_BULK_DOCS, DEFAULT_MAX_BULK_DOCS);
    maxBulkLength = job.getConfiguration().getInt(
        ElasticConstants.MAX_BULK_LENGTH, DEFAULT_MAX_BULK_LENGTH);
  }

}
