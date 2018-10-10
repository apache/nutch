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

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RabbitIndexWriter implements IndexWriter {

  public static final Logger LOG = LoggerFactory
      .getLogger(RabbitIndexWriter.class);

  private String uri;

  private String exchange;
  private String exchangeOptions;

  private String routingKey;

  private int commitSize;
  private String commitMode;

  private String headersStatic;
  private List<String> headersDynamic;

  private boolean binding;
  private String bindingArguments;

  private String queueName;
  private String queueOptions;

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

    uri = parameters.get(RabbitMQConstants.SERVER_URI);

    client = new RabbitMQClient(uri);
    client.openChannel();

    binding = parameters.getBoolean(RabbitMQConstants.BINDING, false);
    if (binding) {
      queueName = parameters.get(RabbitMQConstants.QUEUE_NAME);
      queueOptions = parameters.get(RabbitMQConstants.QUEUE_OPTIONS);

      exchangeOptions = parameters.get(RabbitMQConstants.EXCHANGE_OPTIONS);

      bindingArguments = parameters
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

  /**
   * Returns {@link Map} with the specific parameters the IndexWriter instance can take.
   *
   * @return The values of each row. It must have the form <KEY,<DESCRIPTION,VALUE>>.
   */
  @Override
  public Map<String, Map.Entry<String, Object>> describe() {
    Map<String, Map.Entry<String, Object>> properties = new LinkedHashMap<>();

    properties.put(RabbitMQConstants.SERVER_URI, new AbstractMap.SimpleEntry<>(
        "URI with connection parameters in the form amqp://<username>:<password>@<hostname>:<port>/<virtualHost>",
        this.uri));
    properties.put(RabbitMQConstants.BINDING, new AbstractMap.SimpleEntry<>(
        "Whether the relationship between an exchange and a queue is created automatically. "
            + "NOTE: Binding between exchanges is not supported.",
        this.binding));
    properties.put(RabbitMQConstants.BINDING_ARGUMENTS,
        new AbstractMap.SimpleEntry<>(
            "Arguments used in binding. It must have the form key1=value1,key2=value2. "
                + "This value is only used when the exchange's type is headers and "
                + "the value of binding property is true. In other cases is ignored.",
            this.bindingArguments));
    properties.put(RabbitMQConstants.EXCHANGE_NAME,
        new AbstractMap.SimpleEntry<>(
            "Name for the exchange where the messages will be sent.",
            this.exchange));
    properties.put(RabbitMQConstants.EXCHANGE_OPTIONS,
        new AbstractMap.SimpleEntry<>(
            "Options used when the exchange is created. Only used when the value of binding property is true. "
                + "It must have the form type=<type>,durable=<durable>",
            this.exchangeOptions));
    properties.put(RabbitMQConstants.QUEUE_NAME, new AbstractMap.SimpleEntry<>(
        "Name of the queue used to create the binding. Only used when the value "
            + "of binding property is true.", this.queueName));
    properties.put(RabbitMQConstants.QUEUE_OPTIONS,
        new AbstractMap.SimpleEntry<>(
            "Options used when the queue is created. Only used when the value of "
                + "binding property is true. It must have the form "
                + "durable=<durable>,exclusive=<exclusive>,auto-delete=<auto-delete>,arguments=<arguments>",
            this.queueOptions));
    properties.put(RabbitMQConstants.ROUTING_KEY, new AbstractMap.SimpleEntry<>(
        "The routing key used to route messages in the exchange. "
            + "It only makes sense when the exchange type is topic or direct.",
        this.routingKey));
    properties.put(RabbitMQConstants.COMMIT_MODE, new AbstractMap.SimpleEntry<>(
        "single if a message contains only one document. "
            + "In this case, a header with the action (write, update or delete) will be added. "
            + "multiple if a message contains all documents.",
        this.commitMode));
    properties.put(RabbitMQConstants.COMMIT_SIZE, new AbstractMap.SimpleEntry<>(
        "Amount of documents to send into each message if the value of commit.mode "
            + "property is multiple. In single mode this value represents "
            + "the amount of messages to be sent.", this.commitSize));
    properties.put(RabbitMQConstants.HEADERS_STATIC,
        new AbstractMap.SimpleEntry<>(
            "Headers to add to each message. It must have the form key1=value1,key2=value2.",
            this.headersStatic));
    properties.put(RabbitMQConstants.HEADERS_DYNAMIC,
        new AbstractMap.SimpleEntry<>(
            "Document's fields to add as headers to each message. "
                + "It must have the form field1,field2. "
                + "Only used when the value of commit.mode property is single",
            this.headersDynamic));

    return properties;
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
