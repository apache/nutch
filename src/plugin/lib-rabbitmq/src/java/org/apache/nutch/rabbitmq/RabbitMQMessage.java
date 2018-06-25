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

import java.util.HashMap;
import java.util.Map;

public class RabbitMQMessage {

  private Map<String, Object> headers = new HashMap<>();
  private byte[] body;

  private String contentType = "application/json";

  public Map<String, Object> getHeaders() {
    return headers;
  }

  public void setHeaders(final Map<String, Object> headers) {
    this.headers = headers;
  }

  public void setHeaders(final String headers) {
    this.headers = RabbitMQOptionParser.parseOptionAndConvertValue(headers);
  }

  public void addHeader(final String key, final Object value) {
    this.headers.put(key, value);
  }

  public byte[] getBody() {
    return body;
  }

  public void setBody(final byte[] body) {
    this.body = body;
  }

  public String getContentType() {
    return contentType;
  }

  public void setContentType(final String contentType) {
    this.contentType = contentType;
  }
}
