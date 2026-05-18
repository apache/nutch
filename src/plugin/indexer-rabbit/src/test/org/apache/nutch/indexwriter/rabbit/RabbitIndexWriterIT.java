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
package org.apache.nutch.indexwriter.rabbit;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.AbstractIndexWriterIT;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.NutchConfiguration;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Integration tests for RabbitIndexWriter using Testcontainers.
 */
@Testcontainers(disabledWithoutDocker = true)
public class RabbitIndexWriterIT extends AbstractIndexWriterIT {

  private static final String RABBITMQ_IMAGE = "rabbitmq:3.13-management";

  @Container
  private static final RabbitMQContainer rabbitContainer =
      new RabbitMQContainer(RABBITMQ_IMAGE);

  private RabbitIndexWriter indexWriter;
  private Configuration conf;

  @Override
  public void setUpIndexWriter() throws Exception {
    conf = NutchConfiguration.create();
    indexWriter = new RabbitIndexWriter();
    indexWriter.setConf(conf);

    Map<String, String> params = new HashMap<>();
    params.put(RabbitMQConstants.SERVER_URI, rabbitContainer.getAmqpUrl());
    params.put(RabbitMQConstants.EXCHANGE_NAME, "nutch-indexer-test");
    params.put(RabbitMQConstants.ROUTING_KEY, "indexer");
    params.put(RabbitMQConstants.COMMIT_MODE, "single");
    params.put(RabbitMQConstants.COMMIT_SIZE, "10");
    params.put(RabbitMQConstants.BINDING, "true");
    params.put(RabbitMQConstants.QUEUE_NAME, "nutch-indexer-queue");
    params.put(RabbitMQConstants.EXCHANGE_OPTIONS, "type=direct,durable=true");
    params.put(RabbitMQConstants.QUEUE_OPTIONS,
        "durable=true,exclusive=false,auto-delete=false");

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
}
