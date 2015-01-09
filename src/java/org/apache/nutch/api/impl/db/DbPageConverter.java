/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.api.impl.db;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.Schema.Field;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.StringUtil;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class DbPageConverter {

  public static Map<String, Object> convertPage(WebPage page, Set<String> fields) {
    Map<String, Object> result = Maps.newHashMap();
    for (Field field : filterFields(page, fields)) {
      Object value = convertField(page, field);
      if (value != null) {
        result.put(field.name(), value);
      }
    }
    return result;
  }

  private static Object convertField(WebPage page, Field field) {
    int index = field.pos();
    if (index < 0) {
      return null;
    }

    Object value = page.get(index);
    if (value == null) {
      return null;
    }

    String fieldName = field.name();
    if (StringUtils.equals(fieldName, "metadata")) {
      return getSimpleMetadata(page);
    }
    if (StringUtils.equals(fieldName, "protocolStatus")) {
      return ProtocolStatusUtils.toString(page.getProtocolStatus());
    }
    if (StringUtils.equals(fieldName, "parseStatus")) {
      return ParseStatusUtils.toString(page.getParseStatus());
    }
    if (StringUtils.equals(fieldName, "signature")) {
      return StringUtil.toHexString(page.getSignature());
    }
    if (StringUtils.equals(fieldName, "content")) {
      return Bytes.toStringBinary(page.getContent());
    }
    if (StringUtils.equals(fieldName, "markers")) {
      return convertToStringsMap(page.getMarkers());
    }
    if (StringUtils.equals(fieldName, "inlinks")) {
      return convertToStringsMap(page.getInlinks());
    }
    if (StringUtils.equals(fieldName, "outlinks")) {
      return convertToStringsMap(page.getOutlinks());
    }

    if (value instanceof Utf8) {
      return value.toString();
    }

    if (value instanceof ByteBuffer) {
      return Bytes.toStringBinary((ByteBuffer) value);
    }

    return value;
  }

  private static Set<Field> filterFields(WebPage page, Set<String> queryFields) {
    List<Field> pageFields = page.getSchema().getFields();
    if (CollectionUtils.isEmpty(queryFields)) {
      return Sets.newHashSet(pageFields);
    }

    Set<Field> filteredFields = Sets.newLinkedHashSet();
    for (Field field : pageFields) {
      if (queryFields.contains(field.name())) {
        filteredFields.add(field);
      }
    }
    return filteredFields;
  }

  private static Map<String, String> getSimpleMetadata(WebPage page) {
    Map<CharSequence, ByteBuffer> metadata = page.getMetadata();
    if (MapUtils.isEmpty(metadata)) {
      return Collections.emptyMap();
    }
    Map<String, String> simpleMeta = Maps.newHashMap();
    for (CharSequence key : metadata.keySet()) {
      simpleMeta.put(key.toString(), Bytes.toStringBinary(metadata.get(key)));
    }
    return simpleMeta;
  }

  private static Map<String, String> convertToStringsMap(Map<?, ?> map) {
    Map<String, String> res = Maps.newHashMap();
    for (Entry<?, ?> entry : map.entrySet()) {
      res.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return res;
  }
}
