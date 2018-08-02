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

//TODO refactor the dependencies out of root ivy file
package org.apache.nutch.indexwriter.kafka;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.indexer.IndexWriter;
import org.apache.nutch.indexer.NutchDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
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

  private String jsonString = null;
  private JsonNode json = null;

  private List<ProducerRecord<String, JsonNode>> inputDocs = new ArrayList<ProducerRecord<String, JsonNode>>(
      10);

  @Override
  public void open(JobConf job, String name) throws IOException {

    host = job.get(KafkaConstants.HOST);
    port = job.getInt(KafkaConstants.PORT, 9092);

    keySerializer = job.get(KafkaConstants.KEY_SERIALIZER,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    valueSerializer = job.get(KafkaConstants.VALUE_SERIALIZER,
        "org.apache.kafka.connect.json.JsonSerializer");
    topic = job.get(KafkaConstants.TOPIC);

    Properties configProperties = new Properties();
    configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        host + ":" + port);
    configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        keySerializer);
    configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        valueSerializer);

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
    } catch (NullPointerException e) {
      LOG.info("All records have been sent to Kakfa on topic {}", topic);
    }
  }

  @Override
  public void close() throws IOException {
    commit();
  }

  @Override
  public String describe() {
    StringBuffer sb = new StringBuffer("KafkaIndexWriter\n");
    sb.append("\t").append(KafkaConstants.HOST).append(" : hostname \n");
    sb.append("\t").append(KafkaConstants.PORT).append(" : port \n");
    sb.append("\t").append(KafkaConstants.INDEX)
        .append(" : Kafka index command \n");
    return sb.toString();
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
    String host = conf.get(KafkaConstants.HOST);
    String port = conf.get(KafkaConstants.PORT);

    if (StringUtils.isBlank(host) && StringUtils.isBlank(port)) {
      String message = "Missing kafka.host and kafka.port. These should be set in nutch-site.xml ";
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
