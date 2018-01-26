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
package org.apache.nutch.metadata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * A multi-valued metadata container.
 */
public class Metadata implements Writable, CreativeCommons, DublinCore,
    HttpHeaders, Nutch, Feed {

  /**
   * A map of all metadata attributes.
   */
  private Map<String, String[]> metadata = null;

  /**
   * Constructs a new, empty metadata.
   */
  public Metadata() {
    metadata = new HashMap<>();
  }

  /**
   * Returns true if named value is multivalued.
   * 
   * @param name
   *          name of metadata
   * @return true is named value is multivalued, false if single value or null
   */
  public boolean isMultiValued(final String name) {
    return metadata.get(name) != null && metadata.get(name).length > 1;
  }

  /**
   * Returns an array of the names contained in the metadata.
   * 
   * @return Metadata names
   */
  public String[] names() {
    return metadata.keySet().toArray(new String[metadata.keySet().size()]);
  }

  /**
   * Get the value associated to a metadata name. If many values are assiociated
   * to the specified name, then the first one is returned.
   * 
   * @param name
   *          of the metadata.
   * @return the value associated to the specified metadata name.
   */
  public String get(final String name) {
    String[] values = metadata.get(name);
    if (values == null) {
      return null;
    } else {
      return values[0];
    }
  }

  /**
   * Get the values associated to a metadata name.
   * 
   * @param name
   *          of the metadata.
   * @return the values associated to a metadata name.
   */
  public String[] getValues(final String name) {
    return _getValues(name);
  }

  private String[] _getValues(final String name) {
    String[] values = metadata.get(name);
    if (values == null) {
      values = new String[0];
    }
    return values;
  }

  /**
   * Add a metadata name/value mapping. Add the specified value to the list of
   * values associated to the specified metadata name.
   * 
   * @param name
   *          the metadata name.
   * @param value
   *          the metadata value.
   */
  public void add(final String name, final String value) {
    String[] values = metadata.get(name);
    if (values == null) {
      set(name, value);
    } else {
      String[] newValues = new String[values.length + 1];
      System.arraycopy(values, 0, newValues, 0, values.length);
      newValues[newValues.length - 1] = value;
      metadata.put(name, newValues);
    }
  }

  /**
   * Add all name/value mappings (merge two metadata mappings). If a name
   * already exists in current metadata the values are added to existing values.
   *
   * @param metadata
   *          other Metadata to be merged
   */
  public void addAll(Metadata metadata) {
    for (String name : metadata.names()) {
      String[] addValues = metadata.getValues(name);
      if (addValues == null)
        continue;
      String[] oldValues = this.metadata.get(name);
      if (oldValues == null) {
        this.metadata.put(name, addValues);
      } else {
        String[] newValues = new String[oldValues.length + addValues.length];
        System.arraycopy(oldValues, 0, newValues, 0, oldValues.length);
        System.arraycopy(addValues, 0, newValues, oldValues.length,
            addValues.length);
        this.metadata.put(name, newValues);
      }
    }
  }

  /**
   * Copy All key-value pairs from properties.
   * 
   * @param properties
   *          properties to copy from
   */
  public void setAll(Properties properties) {
    Enumeration<?> names = properties.propertyNames();
    while (names.hasMoreElements()) {
      String name = (String) names.nextElement();
      metadata.put(name, new String[] { properties.getProperty(name) });
    }
  }

  /**
   * Set metadata name/value. Associate the specified value to the specified
   * metadata name. If some previous values were associated to this name, they
   * are removed.
   * 
   * @param name
   *          the metadata name.
   * @param value
   *          the metadata value.
   */
  public void set(String name, String value) {
    metadata.put(name, new String[] { value });
  }

  /**
   * Remove a metadata and all its associated values.
   * 
   * @param name
   *          metadata name to remove
   */
  public void remove(String name) {
    metadata.remove(name);
  }

  /**
   * Returns the number of metadata names in this metadata.
   * 
   * @return number of metadata names
   */
  public int size() {
    return metadata.size();
  }

  /** Remove all mappings from metadata. */
  public void clear() {
    metadata.clear();
  }

  public boolean equals(Object o) {

    if (o == null) {
      return false;
    }

    Metadata other = null;
    try {
      other = (Metadata) o;
    } catch (ClassCastException cce) {
      return false;
    }

    if (other.size() != size()) {
      return false;
    }

    String[] names = names();
    for (int i = 0; i < names.length; i++) {
      String[] otherValues = other._getValues(names[i]);
      String[] thisValues = _getValues(names[i]);
      if (otherValues.length != thisValues.length) {
        return false;
      }
      for (int j = 0; j < otherValues.length; j++) {
        if (!otherValues[j].equals(thisValues[j])) {
          return false;
        }
      }
    }
    return true;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    String[] names = names();
    for (int i = 0; i < names.length; i++) {
      String[] values = _getValues(names[i]);
      for (int j = 0; j < values.length; j++) {
        buf.append(names[i]).append("=").append(values[j]).append(" ");
      }
    }
    return buf.toString();
  }

  public final void write(DataOutput out) throws IOException {
    out.writeInt(size());
    String[] values = null;
    String[] names = names();
    for (int i = 0; i < names.length; i++) {
      Text.writeString(out, names[i]);
      values = _getValues(names[i]);
      int cnt = 0;
      for (int j = 0; j < values.length; j++) {
        if (values[j] != null)
          cnt++;
      }
      out.writeInt(cnt);
      for (int j = 0; j < values.length; j++) {
        if (values[j] != null) {
          Text.writeString(out, values[j]);
        }
      }
    }
  }

  public final void readFields(DataInput in) throws IOException {
    int keySize = in.readInt();
    String key;
    for (int i = 0; i < keySize; i++) {
      key = Text.readString(in);
      int valueSize = in.readInt();
      for (int j = 0; j < valueSize; j++) {
        add(key, Text.readString(in));
      }
    }
  }

}
