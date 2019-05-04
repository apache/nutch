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

import org.apache.hadoop.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class IndexWriterParams extends HashMap<String, String> {

  /**
   * Constructs a new <tt>HashMap</tt> with the same mappings as the
   * specified <tt>Map</tt>.  The <tt>HashMap</tt> is created with
   * default load factor (0.75) and an initial capacity sufficient to
   * hold the mappings in the specified <tt>Map</tt>.
   *
   * @param m the map whose mappings are to be placed in this map
   * @throws NullPointerException if the specified map is null
   */
  public IndexWriterParams(Map<? extends String, ? extends String> m) {
    super(m);
  }

  public String get(String name, String defaultValue) {
    return this.getOrDefault(name, defaultValue);
  }

  public boolean getBoolean(String name, boolean defaultValue) {
    String value;
    if ((value = this.get(name)) != null && !"".equals(value)) {
      return Boolean.parseBoolean(value);
    }

    return defaultValue;
  }

  public long getLong(String name, long defaultValue) {
    String value;
    if ((value = this.get(name)) != null && !"".equals(value)) {
      return Long.parseLong(value);
    }

    return defaultValue;
  }

  public int getInt(String name, int defaultValue) {
    String value;
    if ((value = this.get(name)) != null && !"".equals(value)) {
      return Integer.parseInt(value);
    }

    return defaultValue;
  }

  public String[] getStrings(String name) {
    String value = this.get(name);
    return StringUtils.getStrings(value);
  }

  public String[] getStrings(String name, String... defaultValue) {
    String value;
    if ((value = this.get(name)) != null && !"".equals(value)) {
      return StringUtils.getStrings(value);
    }
    return defaultValue;
  }
}
