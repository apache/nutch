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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexWriterParams;
import org.apache.nutch.indexer.NutchDocument;

import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.indexer.IndexWriter;

import org.apache.nutch.indexer.NutchField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class RabbitIndexWriter implements IndexWriter {

  private String serverHost;
  private int serverPort;
  private String serverVirtualHost;
  private String serverUsername;
  private String serverPassword;

  private String exchangeServer;
  private String exchangeType;

  private String queueName;
  private boolean queueDurable;
  private String queueRoutingKey;

  private int commitSize;

  private static final Logger LOG = LoggerFactory
          .getLogger(MethodHandles.lookup().lookupClass());

  private Configuration config;

  private RabbitMessage rabbitMessage = new RabbitMessage();

  private Channel channel;
  private Connection connection;

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
  }

  @Override
  public void open(JobConf JobConf, String name) throws IOException {
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
    serverHost = parameters.get(RabbitMQConstants.SERVER_HOST, "localhost");
    serverPort = parameters.getInt(RabbitMQConstants.SERVER_PORT, 5672);
    serverVirtualHost = parameters.get(RabbitMQConstants.SERVER_VIRTUAL_HOST, null);

    serverUsername = parameters.get(RabbitMQConstants.SERVER_USERNAME, "admin");
    serverPassword = parameters.get(RabbitMQConstants.SERVER_PASSWORD, "admin");

    exchangeServer = parameters.get(RabbitMQConstants.EXCHANGE_SERVER, "nutch.exchange");
    exchangeType = parameters.get(RabbitMQConstants.EXCHANGE_TYPE, "direct");

    queueName = parameters.get(RabbitMQConstants.QUEUE_NAME, "nutch.queue");
    queueDurable = parameters.getBoolean(RabbitMQConstants.QUEUE_DURABLE, true);
    queueRoutingKey = parameters.get(RabbitMQConstants.QUEUE_ROUTING_KEY, "nutch.key");

    commitSize = parameters.getInt(RabbitMQConstants.COMMIT_SIZE, 250);

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(serverHost);
    factory.setPort(serverPort);

    if (serverVirtualHost != null) {
      factory.setVirtualHost(serverVirtualHost);
    }

    factory.setUsername(serverUsername);
    factory.setPassword(serverPassword);

    try {
      connection = factory.newConnection(UUID.randomUUID().toString());
      channel = connection.createChannel();

      channel.exchangeDeclare(exchangeServer, exchangeType, true);
      channel.queueDeclare(queueName, queueDurable, false, false, null);
      channel.queueBind(queueName, exchangeServer, queueRoutingKey);

    } catch (TimeoutException | IOException ex) {
      throw makeIOException(ex);
    }
  }

  @Override
  public void update(NutchDocument doc) throws IOException {
    RabbitDocument rabbitDocument = new RabbitDocument();

    for (final Map.Entry<String, NutchField> e : doc) {
      RabbitDocument.RabbitDocumentField field = new RabbitDocument.RabbitDocumentField(
              e.getKey(),
              e.getValue().getWeight(),
              e.getValue().getValues());
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
      channel.basicPublish(exchangeServer, queueRoutingKey, null, rabbitMessage.getBytes());
    }
    rabbitMessage.clear();
  }

  @Override
  public void write(NutchDocument doc) throws IOException {
    RabbitDocument rabbitDocument = new RabbitDocument();

    for (final Map.Entry<String, NutchField> e : doc) {
      RabbitDocument.RabbitDocumentField field = new RabbitDocument.RabbitDocumentField(
              e.getKey(),
              e.getValue().getWeight(),
              e.getValue().getValues());
      rabbitDocument.addField(field);
    }
    rabbitDocument.setDocumentBoost(doc.getWeight());

    rabbitMessage.addDocToWrite(rabbitDocument);

    if (rabbitMessage.size() >= commitSize) {
      commit();
    }
  }

  @Override
  public void close() throws IOException {
    commit();//TODO: This is because indexing job never call commit method. It should be fixed.
    try {
      if(channel.isOpen()) {
        channel.close();
      }
      if(connection.isOpen()) {
        connection.close();
      }
    } catch (IOException | TimeoutException e) {
      throw makeIOException(e);
    }
  }

  @Override
  public void delete(String url) throws IOException {
    rabbitMessage.addDocToDelete(url);

    if (rabbitMessage.size() >= commitSize) {
      commit();
    }
  }

  private static IOException makeIOException(Exception e) {
    return new IOException(e);
  }

  public String describe() {
    StringBuilder sb = new StringBuilder("RabbitIndexWriter\n");
    sb.append("\t").append(RabbitMQConstants.SERVER_HOST).append(" : Host of RabbitMQ server\n");
    sb.append("\t").append(RabbitMQConstants.SERVER_PORT).append(" : Port of RabbitMQ server\n");
    sb.append("\t").append(RabbitMQConstants.SERVER_VIRTUAL_HOST).append(" : Virtualhost name\n");
    sb.append("\t").append(RabbitMQConstants.SERVER_USERNAME).append(" : Username for authentication\n");
    sb.append("\t").append(RabbitMQConstants.SERVER_PASSWORD).append(" : Password for authentication\n");
    sb.append("\t").append(RabbitMQConstants.COMMIT_SIZE).append(" : Buffer size when sending to RabbitMQ (default 250)\n");
    return sb.toString();
  }
}
