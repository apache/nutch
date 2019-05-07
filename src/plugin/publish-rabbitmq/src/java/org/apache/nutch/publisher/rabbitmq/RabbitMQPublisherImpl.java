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
import org.apache.nutch.rabbitmq.RabbitMQClient;
import org.apache.nutch.rabbitmq.RabbitMQMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RabbitMQPublisherImpl implements NutchPublisher {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private String exchange;
  private String routingKey;

  private String headersStatic;

  private RabbitMQClient client;

  @Override
  public boolean setConfig(Configuration conf) {
    try {
      exchange = conf.get(RabbitMQConstants.EXCHANGE_NAME);
      routingKey = conf.get(RabbitMQConstants.ROUTING_KEY);
      headersStatic = conf.get(RabbitMQConstants.HEADERS_STATIC, "");

      String uri = conf.get(RabbitMQConstants.SERVER_URI);
      client = new RabbitMQClient(uri);

      client.openChannel();

      boolean binding = conf.getBoolean(RabbitMQConstants.BINDING, false);
      if (binding) {
        String queueName = conf.get(RabbitMQConstants.QUEUE_NAME);
        String queueOptions = conf.get(RabbitMQConstants.QUEUE_OPTIONS);

        String exchangeOptions = conf.get(RabbitMQConstants.EXCHANGE_OPTIONS);

        String bindingArguments = conf
            .get(RabbitMQConstants.BINDING_ARGUMENTS, "");

        client.bind(exchange, exchangeOptions, queueName, queueOptions,
            routingKey, bindingArguments);
      }

      LOG.info("Configured RabbitMQ publisher");
      return true;
    } catch (Exception e) {
      LOG.error("Could not initialize RabbitMQ publisher - {}",
          StringUtils.stringifyException(e));
      return false;
    }
  }

  @Override
  public void publish(Object event, Configuration conf) {
    try {
      RabbitMQMessage message = new RabbitMQMessage();
      message.setBody(getJSONString(event).getBytes());
      message.setHeaders(headersStatic);
      client.publish(exchange, routingKey, message);
    } catch (Exception e) {
      LOG.error("Error occured while publishing - {}",
          StringUtils.stringifyException(e));
    }
  }

  private String getJSONString(Object obj) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      LOG.error("Error converting event object to JSON String - {}",
          StringUtils.stringifyException(e));
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
