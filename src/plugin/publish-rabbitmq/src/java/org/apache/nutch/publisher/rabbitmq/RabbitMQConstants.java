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

interface RabbitMQConstants {
  String RABBIT_PREFIX = "rabbitmq.publisher.";

  String SERVER_URI = RABBIT_PREFIX + "server.uri";

  String EXCHANGE_NAME = RABBIT_PREFIX + "exchange.name";

  String EXCHANGE_OPTIONS = RABBIT_PREFIX + "exchange.options";

  String QUEUE_NAME = RABBIT_PREFIX + "queue.name";

  String QUEUE_OPTIONS = RABBIT_PREFIX + "queue.options";

  String ROUTING_KEY = RABBIT_PREFIX + "routingkey";


  String BINDING = RABBIT_PREFIX + "binding";

  String BINDING_ARGUMENTS = RABBIT_PREFIX + "binding.arguments";


  String HEADERS_STATIC = RABBIT_PREFIX + "headers.static";
}
