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
package org.apache.nutch.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Client for RabbitMQ
 */
public class RabbitMQClient {

  private final static String DEFAULT_EXCHANGE_NAME = "";
  private final static String DEFAULT_EXCHANGE_TYPE = "direct";
  private final static String DEFAULT_EXCHANGE_DURABLE = "true";

  private final static String DEFAULT_QUEUE_NAME = "nutch.queue";
  private final static String DEFAULT_QUEUE_DURABLE = "true";
  private final static String DEFAULT_QUEUE_EXCLUSIVE = "false";
  private final static String DEFAULT_QUEUE_AUTO_DELETE = "false";
  private final static String DEFAULT_QUEUE_ARGUMENTS = "";

  private final static String DEFAULT_ROUTING_KEY = DEFAULT_QUEUE_NAME;

  private Connection connection;
  private Channel channel;

  /**
   * Builds a new instance of {@link RabbitMQClient}
   *
   * @param serverHost        The server host.
   * @param serverPort        The server port.
   * @param serverVirtualHost The virtual host into the RabbitMQ server.
   * @param serverUsername    The username to access the server.
   * @param serverPassword    The password to access the server.
   * @throws IOException It is thrown if there is some issue during the connection creation.
   */
  public RabbitMQClient(String serverHost, int serverPort,
      String serverVirtualHost, String serverUsername, String serverPassword)
      throws IOException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(getValue(serverHost, "localhost"));
    factory.setPort(getValue(serverPort, 5672));

    factory.setVirtualHost(getValue(serverVirtualHost, "/"));

    factory.setUsername(getValue(serverUsername, "guest"));
    factory.setPassword(getValue(serverPassword, "guest"));

    try {
      connection = factory.newConnection();
    } catch (TimeoutException e) {
      throw makeIOException(e);
    }
  }

  /**
   * Builds a new instance of {@link RabbitMQClient}
   *
   * @param uri The connection parameters in the form amqp://userName:password@hostName:portNumber/virtualHost
   * @throws IOException It is thrown if there is some issue during the connection creation.
   */
  public RabbitMQClient(String uri) throws IOException {
    ConnectionFactory factory = new ConnectionFactory();

    try {
      factory.setUri(uri);

      connection = factory.newConnection();
    } catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException | TimeoutException e) {
      throw makeIOException(e);
    }
  }

  /**
   * Opens a new channel into the opened connection.
   *
   * @throws IOException It is thrown if there is some issue during the channel creation.
   */
  public void openChannel() throws IOException {
    channel = connection.createChannel();
  }

  /**
   * Creates a relationship between an exchange and a queue.
   *
   * @param exchangeName     The exchange's name.
   * @param exchangeOptions  Options used when the exchange is created.
   *                         <br />
   *                         It must have the form type={type},durable={durable} where:
   *                         <ul>
   *                         <li>{type} is fanout, direct, headers or topic</li>
   *                         <li>{durable} is true or false</li>
   *                         </ul>
   * @param queueName        The queue's name.
   * @param queueOptions     Options used when the queue is created.
   *                         <br />
   *                         It must have the form durable={type},exclusive={durable},auto-delete={durable},arguments={durable} where:
   *                         <ul>
   *                         <li>durable is true or false</li>
   *                         <li>exclusive is true or false</li>
   *                         <li>auto-delete is true or false</li>
   *                         <li>arguments must have the for {key1:value1;key2:value2}</li>
   *                         </ul>
   * @param bindingKey       The routine key to use for the binding.
   * @param bindingArguments This parameter is only used when the exchange's type is headers. In other cases is ignored.
   *                         <br />
   *                         It must have the form key1=value1,key2=value2
   * @throws IOException If there is some issue creating the relationship.
   */
  public void bind(String exchangeName, String exchangeOptions,
      String queueName, String queueOptions, String bindingKey,
      String bindingArguments) throws IOException {
    String exchangeType = exchangeDeclare(exchangeName, exchangeOptions);
    queueDeclare(queueName, queueOptions);

    switch (exchangeType) {
    case "fanout":
      channel.queueBind(queueName, exchangeName, "");
      break;
    case "direct":
      channel.queueBind(queueName, exchangeName,
          getValue(bindingKey, DEFAULT_ROUTING_KEY));
      break;
    case "headers":
      channel.queueBind(queueName, exchangeName, "",
          RabbitMQOptionParser.parseOptionAndConvertValue(bindingArguments));
      break;
    case "topic":
      channel.queueBind(queueName, exchangeName,
          getValue(bindingKey, DEFAULT_ROUTING_KEY));
      break;
    default:
      break;
    }
  }

  /**
   * Publishes a new message over an exchange.
   *
   * @param exchangeName The exchange's name where the message will be published.
   * @param routingKey   The routing key used to route the message in the exchange.
   * @param message      The message itself.
   * @throws IOException If there is some issue publishing the message.
   */
  public void publish(String exchangeName, String routingKey,
      RabbitMQMessage message) throws IOException {
    channel.basicPublish(getValue(exchangeName, DEFAULT_EXCHANGE_NAME),
        getValue(routingKey, DEFAULT_ROUTING_KEY),
        new AMQP.BasicProperties.Builder().contentType(message.getContentType())
            .headers(message.getHeaders()).build(), message.getBody());
  }

  /**
   * Closes the channel and the connection with the server.
   *
   * @throws IOException If there is some issue trying to close the channel or connection.
   */
  public void close() throws IOException {
    try {
      channel.close();
      connection.close();
    } catch (TimeoutException e) {
      throw makeIOException(e);
    }
  }

  /**
   * Creates a new exchange into the server with the given name and options.
   *
   * @param name    The exchange's name.
   * @param options Options used when the exchange is created.
   *                <br />
   *                It must have the form type={type},durable={durable} where:
   *                <ul>
   *                <li>{type} is fanout, direct, headers or topic</li>
   *                <li>{durable} is true or false</li>
   *                </ul>
   * @return The exchange's type.
   * @throws IOException If there is some issue creating the exchange.
   */
  private String exchangeDeclare(String name, String options)
      throws IOException {
    Map<String, String> values = RabbitMQOptionParser.parseOption(options);

    String type = values.getOrDefault("type", DEFAULT_EXCHANGE_TYPE);

    channel.exchangeDeclare(getValue(name, DEFAULT_EXCHANGE_NAME), type, Boolean
        .parseBoolean(
            values.getOrDefault("durable", DEFAULT_EXCHANGE_DURABLE)));

    return type;
  }

  /**
   * Creates a queue into the server with the given name and options.
   *
   * @param name    The queue's name.
   * @param options Options used when the queue is created.
   *                <br />
   *                It must have the form durable={durable},exclusive={exclusive},auto-delete={auto-delete},arguments={arguments} where:
   *                <ul>
   *                <li>durable is true or false</li>
   *                <li>exclusive is true or false</li>
   *                <li>auto-delete is true or false</li>
   *                <li>arguments must have the for {key1:value1;key2:value2}</li>
   *                </ul>
   * @throws IOException If there is some issue creating the queue.
   */
  private void queueDeclare(String name, String options) throws IOException {
    Map<String, String> values = RabbitMQOptionParser.parseOption(options);

    channel.queueDeclare(getValue(name, DEFAULT_QUEUE_NAME), Boolean
            .parseBoolean(values.getOrDefault("durable", DEFAULT_QUEUE_DURABLE)),
        Boolean.parseBoolean(
            values.getOrDefault("exclusive", DEFAULT_QUEUE_EXCLUSIVE)), Boolean
            .parseBoolean(
                values.getOrDefault("auto-delete", DEFAULT_QUEUE_AUTO_DELETE)),
        RabbitMQOptionParser.parseSubOption(
            values.getOrDefault("arguments", DEFAULT_QUEUE_ARGUMENTS)));
  }

  private static String getValue(String value, String defaultValue) {
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    return value;
  }

  private static Integer getValue(Integer value, Integer defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    return value;
  }

  private static IOException makeIOException(Exception e) {
    return new IOException(e);
  }
}
