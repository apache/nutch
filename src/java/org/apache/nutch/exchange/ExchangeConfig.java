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
package org.apache.nutch.exchange;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.Map;

public class ExchangeConfig {
  private final String id;

  private final String clazz;

  private final String[] writersIDs;

  private final Map<String, String> parameters;

  private ExchangeConfig(String id, String clazz, String[] writersIDs,
      Map<String, String> parameters) {
    this.id = id;
    this.clazz = clazz;
    this.writersIDs = writersIDs;
    this.parameters = parameters;
  }

  public static ExchangeConfig getInstance(Element element) {
    String id = element.getAttribute("id");
    String clazz = element.getAttribute("class");

    //Getting the writers IDs
    NodeList writerList = element.getElementsByTagName("writer");
    String[] writers = new String[writerList.getLength()];
    for (int i = 0; i < writerList.getLength(); i++) {
      writers[i] = ((Element) writerList.item(i)).getAttribute("id");
    }

    //Getting params
    NodeList paramList = element.getElementsByTagName("param");
    Map<String, String> paramsMap = new HashMap<>();
    for (int i = 0; i < paramList.getLength(); i++) {
      Element param = (Element) paramList.item(i);
      paramsMap.put(param.getAttribute("name"), param.getAttribute("value"));
    }

    return new ExchangeConfig(id, clazz, writers, paramsMap);
  }

  public String getId() {
    return id;
  }

  public String getClazz() {
    return clazz;
  }

  String[] getWritersIDs() {
    return writersIDs;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }
}
