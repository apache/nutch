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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;

import org.apache.nutch.indexer.IndexWriter;

import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.rabbitmq.RabbitMQClient;
import org.apache.nutch.rabbitmq.RabbitMQMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RabbitIndexWriter implements IndexWriter {

  public static final Logger LOG = LoggerFactory
      .getLogger(RabbitIndexWriter.class);

  private String exchange;
  private String routingKey;

  private int commitSize;
  private String commitMode;

  private String headersStatic;
  private List<String> headersDynamic;

  private Configuration config;

  private RabbitMessage rabbitMessage = new RabbitMessage();

  private RabbitMQClient client;

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
  }

  @Override
  public void open(Configuration conf, String name) throws IOException {
    //Implementation not required
  }

  /**
   * Initializes the internal variables from a given index writer configuration.
   *
   * @param parameters Params from the index writer configuration.
   * @throws IOException Some exception thrown by writer.
   */
  @Override
  public void open(IndexWriterParams parameters) throws IOException {
    exchange = parameters.get(RabbitMQConstants.EXCHANGE_NAME);
    routingKey = parameters.get(RabbitMQConstants.ROUTING_KEY);

    commitSize = parameters.getInt(RabbitMQConstants.COMMIT_SIZE, 250);
    commitMode = parameters.get(RabbitMQConstants.COMMIT_MODE, "multiple");

    headersStatic = parameters.get(RabbitMQConstants.HEADERS_STATIC, "");
    headersDynamic = Arrays
        .asList(parameters.getStrings(RabbitMQConstants.HEADERS_DYNAMIC, ""));

    String uri = parameters.get(RabbitMQConstants.SERVER_URI);

    client = new RabbitMQClient(uri);
    client.openChannel();

    boolean binding = parameters.getBoolean(RabbitMQConstants.BINDING, false);
    if (binding) {
      String queueName = parameters.get(RabbitMQConstants.QUEUE_NAME);
      String queueOptions = parameters.get(RabbitMQConstants.QUEUE_OPTIONS);

      String exchangeOptions = parameters.get(RabbitMQConstants.EXCHANGE_OPTIONS);

      String bindingArguments = parameters
          .get(RabbitMQConstants.BINDING_ARGUMENTS, "");

      client
          .bind(exchange, exchangeOptions, queueName, queueOptions, routingKey,
              bindingArguments);
    }
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    RabbitDocument rabbitDocument = new RabbitDocument();

    for (final Map.Entry<String, NutchField> e : doc) {
      RabbitDocument.RabbitDocumentField field = new RabbitDocument.RabbitDocumentField(
          e.getKey(), e.getValue().getWeight(), e.getValue().getValues());
      rabbitDocument.addField(field);
    }
    rabbitDocument.setDocumentBoost(doc.getWeight());

    rabbitMessage.addDocToWrite(rabbitDocument);

    if (rabbitMessage.size() >= commitSize) {
      commit();
    }
  }

  @Override
  public void delete(String url) throws IOException {
    rabbitMessage.addDocToDelete(url);

    if (rabbitMessage.size() >= commitSize) {
      commit();
    }
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    RabbitDocument rabbitDocument = new RabbitDocument();

    for (final Map.Entry<String, NutchField> e : doc) {
      RabbitDocument.RabbitDocumentField field = new RabbitDocument.RabbitDocumentField(
          e.getKey(), e.getValue().getWeight(), e.getValue().getValues());
      rabbitDocument.addField(field);
    }
    rabbitDocument.setDocumentBoost(doc.getWeight());

    rabbitMessage.addDocToUpdate(rabbitDocument);
    if (rabbitMessage.size() >= commitSize) {
      commit();
    }
  }

  @Override
  public void commit() throws IOException {
    if (!rabbitMessage.isEmpty()) {

      if ("single".equals(commitMode)) {
        // The messages to delete
        for (String s : rabbitMessage.getDocsToDelete()) {
          RabbitMQMessage message = new RabbitMQMessage();
          message.setBody(s.getBytes());
          message.setHeaders(headersStatic);
          message.addHeader("action", "delete");
          client.publish(exchange, routingKey, message);
        }

        // The messages to update
        for (RabbitDocument rabbitDocument : rabbitMessage.getDocsToUpdate()) {
          RabbitMQMessage message = new RabbitMQMessage();
          message.setBody(rabbitDocument.getBytes());
          addHeaders(message, rabbitDocument);
          message.addHeader("action", "update");
          client.publish(exchange, routingKey, message);
        }

        // The messages to write
        for (RabbitDocument rabbitDocument : rabbitMessage.getDocsToWrite()) {
          RabbitMQMessage message = new RabbitMQMessage();
          message.setBody(rabbitDocument.getBytes());
          addHeaders(message, rabbitDocument);
          message.addHeader("action", "write");
          client.publish(exchange, routingKey, message);
        }
      } else {
        RabbitMQMessage message = new RabbitMQMessage();
        message.setBody(rabbitMessage.getBytes());
        message.setHeaders(headersStatic);
        client.publish(exchange, routingKey, message);
      }
    }
    rabbitMessage.clear();
  }

  @Override
  public void close() throws IOException {
    commit(); //TODO: This is because indexing job never call commit method. It should be fixed.
    client.close();
  }

  public String describe() {
    StringBuffer sb = new StringBuffer("RabbitIndexWriter\n");
    sb.append("\t").append(RabbitMQConstants.SERVER_URI)
        .append(" : URI of RabbitMQ server\n");
    sb.append("\t").append(RabbitMQConstants.BINDING).append(
        " : If binding is created automatically or not (default true)\n");
    sb.append("\t").append(RabbitMQConstants.BINDING_ARGUMENTS)
        .append(" : Arguments used in binding\n");
    sb.append("\t").append(RabbitMQConstants.EXCHANGE_NAME)
        .append(" : Exchange's name\n");
    sb.append("\t").append(RabbitMQConstants.EXCHANGE_OPTIONS)
        .append(" : Exchange's options\n");
    sb.append("\t").append(RabbitMQConstants.QUEUE_NAME)
        .append(" : Queue's name\n");
    sb.append("\t").append(RabbitMQConstants.QUEUE_OPTIONS)
        .append(" : Queue's options\n");
    sb.append("\t").append(RabbitMQConstants.ROUTING_KEY)
        .append(" : Routing key\n");
    sb.append("\t").append(RabbitMQConstants.COMMIT_SIZE)
        .append(" : Buffer size when sending to RabbitMQ (default 250)\n");
    sb.append("\t").append(RabbitMQConstants.COMMIT_MODE)
        .append(" : The mode to send the documents (default multiple)\n");
    sb.append("\t").append(RabbitMQConstants.HEADERS_STATIC)
        .append(" : Static headers that will be added to the messages\n");
    sb.append("\t").append(RabbitMQConstants.HEADERS_DYNAMIC)
        .append(" : Document's fields added as headers\n");
    return sb.toString();
  }

  private void addHeaders(final RabbitMQMessage message,
      RabbitDocument document) {
    message.setHeaders(headersStatic);

    for (RabbitDocument.RabbitDocumentField rabbitDocumentField : document
        .getFields()) {
      if (headersDynamic.contains(rabbitDocumentField.getKey())) {
        message.addHeader(rabbitDocumentField.getKey(),
            rabbitDocumentField.getValues().get(0));
      }
    }
  }
}
