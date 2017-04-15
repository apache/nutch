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

package org.apache.nutch.publisher.rabbitmq;

import java.lang.invoke.MethodHandles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.publisher.NutchPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMQPublisherImpl implements NutchPublisher{

  private static String EXCHANGE_SERVER;
  private static String EXCHANGE_TYPE;

  private static String HOST
    
  private static int PORT;
  private static String VIRTUAL_HOST;
  private static String USERNAME;
  private static String PASSWORD;

  private static String QUEUE_NAME;
  private static boolean QUEUE_DURABLE;
  private static String QUEUE_ROUTING_KEY;
  
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  
  private static Channel channel;

  @Override
  public boolean setConfig(Configuration conf) {
    try {
      EXCHANGE_SERVER = conf.get("rabbitmq.exchange.server", "fetcher_log");
      EXCHANGE_TYPE = conf.get("rabbitmq.exchange.type", "fanout");

      HOST = conf.get("rabbitmq.host", "localhost");
      PORT = conf.getInt("rabbitmq.port", 5672);
      VIRTUAL_HOST = conf.get("rabbitmq.virtualhost", null);
      USERNAME = conf.get("rabbitmq.username", null);
      PASSWORD = conf.get("rabbitmq.password", null);

      QUEUE_NAME = conf.get("rabbitmq.queue.name", "fanout.queue");
      QUEUE_DURABLE = conf.getBoolean("rabbitmq.queue.durable", true);
      QUEUE_ROUTING_KEY = conf.get("rabbitmq.queue.routingkey", "fanout.key");

      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost(HOST);
      factory.setPort(PORT);

      if(VIRTUAL_HOST != null) {
        factory.setVirtualHost(VIRTUAL_HOST);
      }

      if(USERNAME != null) {
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
      }
    
      Connection connection = factory.newConnection();
      channel = connection.createChannel();
      channel.exchangeDeclare(EXCHANGE_SERVER, EXCHANGE_TYPE);
      channel.queueDeclare(QUEUE_NAME, QUEUE_DURABLE, false, false, null);
      channel.queueBind(QUEUE_NAME, EXCHANGE_SERVER, QUEUE_ROUTING_KEY);

      LOG.info("Configured RabbitMQ publisher");
      return true;
    }catch(Exception e) {
      LOG.error("Could not initialize RabbitMQ publisher - {}", StringUtils.stringifyException(e));
      return false;
    }

  }

  @Override
  public void publish(Object event, Configuration conf) {
    try {
      channel.basicPublish(EXCHANGE_SERVER, QUEUE_ROUTING_KEY, null, getJSONString(event).getBytes());
    } catch (Exception e) {
      LOG.error("Error occured while publishing - {}", StringUtils.stringifyException(e));
    }
  }

  private String getJSONString(Object obj) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      LOG.error("Error converting event object to JSON String - {}", StringUtils.stringifyException(e));
    }
    return null;
  }


  @Override
  public void setConf(Configuration arg0) {
    
  }

  @Override
  public Configuration getConf() {
    return null;
  }

}
