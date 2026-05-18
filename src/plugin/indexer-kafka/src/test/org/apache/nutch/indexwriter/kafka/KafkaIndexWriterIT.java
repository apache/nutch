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
package org.apache.nutch.indexwriter.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.AbstractIndexWriterIT;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.util.NutchConfiguration;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Integration tests for KafkaIndexWriter using Testcontainers.
 */
@Testcontainers(disabledWithoutDocker = true)
public class KafkaIndexWriterIT extends AbstractIndexWriterIT {

  private static final String KAFKA_IMAGE = "apache/kafka-native:3.8.0";
  private static final String TEST_TOPIC = "nutch-indexer-test";

  @Container
  private static final KafkaContainer kafkaContainer =
      new KafkaContainer(KAFKA_IMAGE);

  private KafkaIndexWriter indexWriter;
  private Configuration conf;

  @Override
  public void setUpIndexWriter() throws Exception {
    conf = NutchConfiguration.create();
    indexWriter = new KafkaIndexWriter();
    indexWriter.setConf(conf);

    String bootstrapServers = kafkaContainer.getBootstrapServers();
    String hostPort = bootstrapServers.contains("://")
        ? bootstrapServers.substring(bootstrapServers.indexOf("://") + 3)
        : bootstrapServers;
    String[] parts = hostPort.split(":");
    String host = parts[0];
    int port = Integer.parseInt(parts[1]);

    Map<String, String> params = new HashMap<>();
    params.put(KafkaConstants.HOST, host);
    params.put(KafkaConstants.PORT, String.valueOf(port));
    params.put(KafkaConstants.TOPIC, TEST_TOPIC);
    params.put(KafkaConstants.VALUE_SERIALIZER,
        "org.apache.kafka.connect.json.JsonSerializer");
    params.put(KafkaConstants.KEY_SERIALIZER,
        "org.apache.kafka.common.serialization.StringSerializer");

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
    return false;
  }
}
