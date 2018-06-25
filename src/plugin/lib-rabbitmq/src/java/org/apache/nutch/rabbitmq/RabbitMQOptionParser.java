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

class RabbitMQOptionParser {

  static Map<String, String> parseOption(final String option) {
    Map<String, String> values = new HashMap<>();

    if (option.isEmpty()) {
      return values;
    }

    String[] split = option.split(",");
    for (String s : split) {
      String[] ss = s.split("=");
      values.put(ss[0], ss[1]);
    }

    return values;
  }

  static Map<String, Object> parseOptionAndConvertValue(final String option) {
    Map<String, Object> values = new HashMap<>();

    if (option.isEmpty()) {
      return values;
    }

    String[] split = option.split(",");
    for (String s : split) {
      String[] ss = s.split("=");
      values.put(ss[0], convert(ss[1]));
    }

    return values;
  }

  static Map<String, Object> parseSubOption(final String subOption) {
    Map<String, Object> values = new HashMap<>();

    if (subOption.isEmpty()) {
      return values;
    }

    String[] split = subOption.replaceAll("\\{|}", "").split(";");
    for (String s : split) {
      String[] ss = s.split(":");
      values.put(ss[0], convert(ss[1]));
    }

    return values;
  }

  private static Object convert(String s) {
    try {
      return Integer.parseInt(s);
    } catch (Exception ex) {
      // Do nothing
    }

    try {
      return Float.parseFloat(s);
    } catch (Exception ex) {
      // Do nothing
    }

    if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")) {
      return Boolean.parseBoolean(s);
    }

    return s;
  }
}
