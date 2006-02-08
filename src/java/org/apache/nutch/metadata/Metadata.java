/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

// JDK imports
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

// Commons Lang imports
import org.apache.commons.lang.StringUtils;

// Hadoop imports
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;


/**
 * A syntax tolerant and multi-valued metadata container.
 *
 * All the static String fields declared by this class are used as reference
 * names for syntax correction on meta-data naming.
 *
 * @author Chris Mattmann
 * @author J&eacute;r&ocirc;me Charron
 */
public class Metadata implements CreativeCommons,
                                 DublinCore,
                                 HttpHeaders,
                                 Nutch,
                                 Office,
                                 Writable {
  

  /** Used to format DC dates for the DATE metadata field */
  public final static SimpleDateFormat DATE_FORMAT = 
          new SimpleDateFormat("yyyy-MM-dd");
  
    
  private final static Map NAMES_IDX = new HashMap();
  private static String[] normalized = null;

  // Uses self introspection to fill the metanames index and the
  // metanames list.
  static {
    Field[] fields = Metadata.class.getFields();
    for (int i=0; i<fields.length; i++) {
      int mods = fields[i].getModifiers();
      if (Modifier.isFinal(mods) &&
          Modifier.isPublic(mods) &&
          Modifier.isStatic(mods) &&
          fields[i].getType().equals(String.class)) {
        try {
          String val = (String) fields[i].get(null);
          NAMES_IDX.put(normalize(val), val);
        } catch (Exception e) {
          // Simply ignore...
        }
      }
    }
    normalized = (String[]) NAMES_IDX.keySet().toArray(new String[NAMES_IDX.size()]);
  }
  
  
  /** A map of all metadata attributes */
  private Map metadata = null;

  
  /** Constructs a new, empty metadata. */
  public Metadata() {
    metadata = new HashMap();
  }

  /**
   */
  public boolean isMultiValued(String name) {
    return getValues(name).length > 1;
  }

  /**
   * Returns an array of the names contained in the metadata.
   */
  public String[] names() {
    Iterator iter = metadata.keySet().iterator();
    List names = new ArrayList();
    while(iter.hasNext()) {
      names.add(getNormalizedName((String) iter.next()));
    }
    return (String[]) names.toArray(new String[names.size()]);
  }
  
  /**
   * Get the value associated to a metadata name.
   * If many values are assiociated to the specified name, then the first
   * one is returned.
   *
   * @param name of the metadata.
   * @return the value associated to the specified metadata name.
   */
  public String get(String name) {
    Object values = metadata.get(getNormalizedName(name));
    if ((values != null) && (values instanceof List)) {
      return (String) ((List) values).get(0);
    } else {
      return (String) values;
    }
  }

  /**
   * Get the values associated to a metadata name.
   * @param name of the metadata.
   * @return the values associated to a metadata name.
   */
  public String[] getValues(String name) {
    Object values = metadata.get(getNormalizedName(name));
    if (values != null) {
      if (values instanceof List) {
        List list = (List) values;
        return (String[]) list.toArray(new String[list.size()]);
      } else {
        return new String[] { (String) values };
      }
    }
    return new String[0];
  }
  
  /**
   * Add a metadata name/value mapping.
   * Add the specified value to the list of values associated to the
   * specified metadata name.
   *
   * @param name the metadata name.
   * @param value the metadata value.
   */
  public void add(String name, String value) {
    String normalized = getNormalizedName(name);
    Object values = metadata.get(normalized);
    if (values != null) {
      if (values instanceof String) {
        List list = new ArrayList();
        list.add(values);
        list.add(value);
        metadata.put(normalized, list);
      } else if (values instanceof List) {
        ((List) values).add(value);
      }
    } else {
      metadata.put(normalized, value);
    }
  }

  public void setAll(Properties properties) {
    Enumeration names = properties.propertyNames();
    while (names.hasMoreElements()) {
      String name = (String) names.nextElement();
      set(name, properties.getProperty(name));
    }
  }
  
  /**
   * Set metadata name/value.
   * Associate the specified value to the specified metadata name. If some
   * previous values were associated to this name, they are removed.
   *
   * @param name the metadata name.
   * @param value the metadata value.
   */
  public void set(String name, String value) {
    remove(name);
    add(name, value);
  }

  /**
   * Remove a metadata and all its associated values.
   */
  public void remove(String name) {
    metadata.remove(getNormalizedName(name));
  }
  
  /**
   * Returns the number of metadata names in this metadata.
   */
  public int size() {
    return metadata.size();
  }
  
  // Inherited Javadoc
  public boolean equals(Object o) {
    
    if (o == null) { return false; }
    
    Metadata other = null;
    try {
      other = (Metadata) o;
    } catch (ClassCastException cce) {
      return false;
    }
    
    if (other.size() != size()) { return false; }
    
    String[] names = names();
    for (int i=0; i<names.length; i++) {
      String[] otherValues = other.getValues(names[i]);
      String[] thisValues = getValues(names[i]);
      if (otherValues.length != thisValues.length) {
        return false;
      }
      for (int j=0; j<otherValues.length; j++) {
        if (!otherValues[j].equals(thisValues[j])) {
          return false;
        }
      }
    }
    return true;
  }
  
  
  /**
   * Get the normalized name of metadata attribute name.
   * This method tries to find a well-known metadata name (one of the
   * metadata names defined in this class) that matches the specified name.
   * The matching is error tolerent. For instance,
   * <ul>
   *  <li>content-type gives Content-Type</li>
   *  <li>CoNtEntType  gives Content-Type</li>
   *  <li>ConTnTtYpe   gives Content-Type</li>
   * </ul>
   * If no matching with a well-known metadata name is found, then the original
   * name is returned.
   */
  public static String getNormalizedName(String name) {
    String searched = normalize(name);
    String value = (String) NAMES_IDX.get(searched);

    if ((value == null) && (normalized != null)) {
      int threshold = searched.length() / 3;
      for (int i=0; i<normalized.length && value == null; i++) {
        if (StringUtils.getLevenshteinDistance(searched, normalized[i]) < threshold) {
          value = (String) NAMES_IDX.get(normalized[i]);
        }
      }
    }
    return (value != null) ? value : name;
  }
    
  private final static String normalize(String str) {
    char c;
    StringBuffer buf = new StringBuffer();
    for (int i=0; i<str.length(); i++) {
      c = str.charAt(i);
      if (Character.isLetter(c)) {
        buf.append(Character.toLowerCase(c));
      }
    }
    return buf.toString();
  }

  
  /* ------------------------- *
   * <implementation:Writable> *
   * ------------------------- */
  
  // Inherited Javadoc
  public final void write(DataOutput out) throws IOException {
    out.writeInt(size());
    String[] values = null;
    String[] names = names();
    for (int i=0; i<names.length; i++) {
      UTF8.writeString(out, names[i]);
      values = getValues(names[i]);
      out.writeInt(values.length);
      for (int j=0; j<values.length; j++) {
        UTF8.writeString(out, values[j]);
      }
    }
  }

  // Inherited Javadoc
  public final void readFields(DataInput in) throws IOException {
    int keySize = in.readInt();
    String key;
    for (int i=0; i<keySize; i++) {
      key = UTF8.readString(in);
      int valueSize = in.readInt();
      for (int j=0; j<valueSize; j++) {
        add(key, UTF8.readString(in));
      }
    }
  }

  /* -------------------------- *
   * </implementation:Writable> *
   * -------------------------- */
   
}
