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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.JsonNode;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Sends Nutch documents to a configured Kafka Cluster
 */
public class KafkaIndexWriter implements IndexWriter {
  public static Logger LOG = LoggerFactory.getLogger(KafkaIndexWriter.class);

  private org.apache.kafka.clients.producer.Producer<String, JsonNode> producer;
  private ProducerRecord<String, JsonNode> data;

  private Configuration config;

  private int port = -1;
  private String host = null;
  private String valueSerializer = null;
  private String keySerializer = null;
  private String topic = null;
  private int maxDocCount = -1;

  private String jsonString = null;
  private JsonNode json = null;

  private List<ProducerRecord<String, JsonNode>> inputDocs = null;

  @Override
  public void open(Configuration job, String name) throws IOException {
    //Implementation not required
  }
  
  @Override
  public void open(IndexWriterParams params) throws IOException {
    host = params.get(KafkaConstants.HOST);
    port = params.getInt(KafkaConstants.PORT, 9092);
    
    keySerializer = params.get(KafkaConstants.KEY_SERIALIZER,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    valueSerializer = params.get(KafkaConstants.VALUE_SERIALIZER,
        "org.apache.kafka.connect.json.JsonSerializer");
    topic = params.get(KafkaConstants.TOPIC);
    maxDocCount = params.getInt(KafkaConstants.MAX_DOC_COUNT, 100);

    inputDocs = new ArrayList<ProducerRecord<String, JsonNode>>(maxDocCount);
    
    if (StringUtils.isBlank(host)) {
      String message = "Missing host. It should be set in index-writers.xml";
      message += "\n" + describe();
      LOG.error(message);
      throw new RuntimeException(message);
    }
    
    Properties configProperties = new Properties();
    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        host + ":" + port);
    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        keySerializer);
    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        valueSerializer);

    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
    producer = new KafkaProducer<String, JsonNode>(configProperties);
  }

  @Override
  public void write(NutchDocument doc) throws IOException {

    Map<String, Object> source = new HashMap<String, Object>();

    // Loop through all fields of this doc
    for (String fieldName : doc.getFieldNames()) {
      Set<String> allFieldValues = new HashSet<String>();
      for (Object value : doc.getField(fieldName).getValues()) {
        allFieldValues.add(value.toString());
      }
      String[] fieldValues = allFieldValues
          .toArray(new String[allFieldValues.size()]);
      source.put(fieldName, fieldValues);
    }
    try {
      jsonString = new ObjectMapper().writeValueAsString(source);
      json = new ObjectMapper().readTree(jsonString);
      data = new ProducerRecord<String, JsonNode>(topic, json);

      inputDocs.add(data);
      if (inputDocs.size() == maxDocCount) {
        commit();
      }
    } catch (NullPointerException e) {
      LOG.info("Data is empty, all messages have been sent");
    }
  }

  @Override
  public void delete(String key) throws IOException {
    // Not applicable in Kafka
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
    try {
      for (ProducerRecord<String, JsonNode> datum : inputDocs) {
        producer.send(datum);
      }
      inputDocs.clear();
    } catch (NullPointerException e) {
      LOG.info("All records have been sent to Kakfa on topic {}", topic);
    }
  }

  @Override
  public void close() throws IOException {
    commit();
    producer.close();
  }

  @Override
  public Map<String, Map.Entry<String, Object>> describe() {
    Map<String, Map.Entry<String, Object>> properties = new LinkedHashMap<>();

    properties.put(KafkaConstants.HOST,
            new AbstractMap.SimpleEntry<>(
                    "Location of the host Kafka cluster to connect to using producerConfig",
                    this.host));

    properties.put(KafkaConstants.PORT,
            new AbstractMap.SimpleEntry<>(
                    "The port to connect to using the producerConfig",
                    this.port));

    properties.put(KafkaConstants.TOPIC,
            new AbstractMap.SimpleEntry<>(
                    "Default index to attach to documents",
                    this.topic));

    properties.put(KafkaConstants.KEY_SERIALIZER,
    new AbstractMap.SimpleEntry<>(
            "instruct how to turn the key object the user provides with their ProducerRecord into bytes",
            this.keySerializer));      

    properties.put(KafkaConstants.VALUE_SERIALIZER,
    new AbstractMap.SimpleEntry<>(
            "instruct how to turn the value object the user provides with their ProducerRecord into bytes",
            this.valueSerializer));

    properties.put(KafkaConstants.MAX_DOC_COUNT,
    new AbstractMap.SimpleEntry<>(
            "Maximum number of documents before a commit is forced",
            this.maxDocCount));
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

}
