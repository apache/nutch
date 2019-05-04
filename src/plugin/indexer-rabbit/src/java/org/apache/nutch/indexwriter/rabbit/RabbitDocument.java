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

import com.google.gson.Gson;

import java.util.LinkedList;
import java.util.List;

class RabbitDocument {
  private List<RabbitDocumentField> fields;

  @SuppressWarnings("unused")
  private float documentBoost;

  RabbitDocument() {
    this.fields = new LinkedList<>();
  }

  List<RabbitDocumentField> getFields() {
    return fields;
  }

  void setDocumentBoost(float documentBoost) {
    this.documentBoost = documentBoost;
  }

  void addField(RabbitDocumentField field) {
    fields.add(field);
  }

  byte[] getBytes() {
    Gson gson = new Gson();
    return gson.toJson(this).getBytes();
  }

  static class RabbitDocumentField {
    private String key;
    @SuppressWarnings("unused")
    private float weight;
    private List<Object> values;

    RabbitDocumentField(String key, float weight, List<Object> values) {
      this.key = key;
      this.weight = weight;
      this.values = values;
    }

    public String getKey() {
      return key;
    }

    public List<Object> getValues() {
      return values;
    }
  }
}
