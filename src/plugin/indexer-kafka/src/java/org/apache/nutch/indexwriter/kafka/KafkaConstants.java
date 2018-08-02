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

public interface KafkaConstants {
  public static final String KAFKA_PREFIX = "kafka.";

  public static final String HOST = KAFKA_PREFIX + "host";
  public static final String PORT = KAFKA_PREFIX + "port";
  public static final String INDEX = KAFKA_PREFIX + "index";

  public static final String KEY_SERIALIZER = KAFKA_PREFIX + "key.serializer";
  public static final String VALUE_SERIALIZER = KAFKA_PREFIX
      + "value.serializer";
  public static final String TOPIC = KAFKA_PREFIX + "topic";
}
