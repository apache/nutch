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

interface RabbitMQConstants {

    String SERVER_HOST = "server.host";

    String SERVER_PORT = "server.port";

    String SERVER_VIRTUAL_HOST = "server.virtualhost";

    String SERVER_USERNAME = "server.username";

    String SERVER_PASSWORD = "server.password";

    String EXCHANGE_SERVER = "exchange.server";

    String EXCHANGE_TYPE = "exchange.type";

    String QUEUE_NAME = "queue.name";

    String QUEUE_DURABLE = "queue.durable";

    String QUEUE_ROUTING_KEY = "queue.routingkey";

    String COMMIT_SIZE = "commit.size";
}
