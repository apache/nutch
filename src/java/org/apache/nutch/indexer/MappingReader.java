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
package org.apache.nutch.indexer;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.*;

class MappingReader {

  /**
   * Converts the tag "mapping" to a {@link Map} instance.
   *
   * @param mappingElement The tag "mapping" wrapped into an {@link Element} instance.
   * @return The {@link Map} instance with the actions for mapping the fields.
   */
  static Map<Actions, Map<String, List<String>>> parseMapping(Element mappingElement) {
    Map<Actions, Map<String, List<String>>> parsedMapping = new HashMap<>();

    //Getting rename action
    Node node = mappingElement.getElementsByTagName("rename").item(0);

    if (node != null) {
      NodeList fieldList = ((Element) node).getElementsByTagName("field");

      Map<String, List<String>> fieldsMap = new HashMap<>();

      for (int j = 0; j < fieldList.getLength(); j++) {
        Element field = (Element) fieldList.item(j);
        fieldsMap.put(field.getAttribute("source"), Collections.singletonList(field.getAttribute("dest")));
      }

      parsedMapping.put(Actions.RENAME, fieldsMap);
    }

    //Getting copy action
    node = mappingElement.getElementsByTagName("copy").item(0);

    if (node != null) {
      NodeList fieldList = ((Element) node).getElementsByTagName("field");

      Map<String, List<String>> fieldsMap = new HashMap<>();

      for (int j = 0; j < fieldList.getLength(); j++) {
        Element field = (Element) fieldList.item(j);
        fieldsMap.put(field.getAttribute("source"), Arrays.asList(field.getAttribute("dest").split(",")));
      }

      parsedMapping.put(Actions.COPY, fieldsMap);
    }

    //Getting remove action
    node = mappingElement.getElementsByTagName("remove").item(0);

    if (node != null) {
      NodeList fieldList = ((Element) node).getElementsByTagName("field");

      Map<String, List<String>> fieldsMap = new HashMap<>();

      for (int j = 0; j < fieldList.getLength(); j++) {
        Element field = (Element) fieldList.item(j);
        fieldsMap.put(field.getAttribute("source"), null);
      }

      parsedMapping.put(Actions.REMOVE, fieldsMap);
    }

    return parsedMapping;
  }

  /**
   * Available actions for mapping fields.
   */
  enum Actions {
    RENAME, COPY, REMOVE
  }
}
