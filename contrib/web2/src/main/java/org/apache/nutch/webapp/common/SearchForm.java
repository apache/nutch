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
package org.apache.nutch.webapp.common;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * SearchForm is a representation of query parameters submitted as part of
 * search form. It provides functionality to preserve all required parameters
 * for example when creating link to "next page"
 *
 * Plugins participating in search
 *
 * @see org.apache.nutch.webapp.extension.PreSearch,
 * @see org.apache.nutch.webapp.extension.Search,
 * @see org.apache.nutch.webapp.extension.PostSearch
 *
 * can by reading (existing) or setting values to/from this map make sure that
 * those parameters are persisted to forms/links requiring them without any
 * special tricks.
 *
 */
public class SearchForm {

  /**
   * General HTTP parameter name wich contains hits per page value
   */
  public static final String NAME_HITSPERPAGE = "hitsPerPage";

  public static final String NAME_HITSPERDUP = "hitsPerDup";

  public static final String NAME_START = "start";

  public static final String NAME_QUERYSTRING = "query";

  public static final String NAME_SORTCOLUMN = "sort";

  public static final String NAME_SORTREVERSE = "reverse";

  public static final String NAME_DUPCOLUMN = "dup";

  private static final long serialVersionUID = 1L;

  Map o_values;

  Map n_values = new HashMap();

  ArrayList active;

  /**
   * KeyValue presents a class that holds key value pair.
   *
   */
  public static class KeyValue {

    String key;

    String value;

    /**
     * Construct new KeyValue with provided values
     * 
     * @param key
     *          the key
     * @param value
     *          the value
     */
    public KeyValue(String key, String value) {
      this.key = key;
      this.value = value;
    }

    /**
     * Gets the key
     * 
     * @return contained key
     */
    public String getKey() {
      return key;
    }

    /**
     * Gets the value
     * 
     * @return contained value
     */
    public String getValue() {
      return value;
    }

    /**
     * Sets the value
     * 
     * @param value
     *          The value to set.
     */
    public void setValue(String value) {
      this.value = value;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    public String toString() {
      return value.toString();
    }
  }

  /**
   * Construct new SearchForm based on provided map map values are String arrays
   * 
   * @param parameters
   *          original map from request
   */
  public SearchForm(Map parameters) {
    o_values = parameters;
    active = new ArrayList();
  }

  /**
   * Sets value for provided key.
   *
   * @param key
   *          the key to set the value on
   * @param value
   *          the value to set
   */
  public void setValue(String key, String value) {
    if (n_values.containsKey(key)) {
      ((KeyValue) n_values.get(key)).setValue(value);
    } else {
      n_values.put(key, new KeyValue(key, value));
    }
    if(!active.contains(n_values.get(key)))
      active.add(n_values.get(key));
  }

  /**
   * Gets the real value contained in keyed KeyValue.
   *
   * @param key
   * @return
   */
  public String getValue(String key) {
    if (n_values.containsKey(key) && !active.contains(n_values.get(key))) {
      active.add(n_values.get(key));
      return (String) ((KeyValue) n_values.get(key)).getValue();
    } else {
      return null;
    }
  }

  /**
   * Gets the String value contained in keyed KeyValue.
   *
   * @param key
   * @return Object.toString()
   */
  public String getValueString(String key) {
    if (n_values.containsKey(key)) {
      active.add(n_values.get(key));
    } else if (o_values.containsKey(key)) {
      // get only the 1st parameter
      n_values.put(key, new KeyValue(key, ((String[]) o_values.get(key))[0]));
      active.add(n_values.get(key));
    } else {
      return null;
    }

    try {
      return (String) ((KeyValue) n_values.get(key)).getValue();
    } catch (ClassCastException e) {
      return ((KeyValue) n_values.get(key)).getValue().toString();
    }
  }

  /**
   * Remove named key (and the KeyValue).
   *
   * @param key
   * @return
   */
  public KeyValue remove(String key) {
    if (n_values.containsKey(key)) {
      active.remove(n_values.get(key));
      return (KeyValue) n_values.remove(key);
    } else {
      return null;
    }
  }

  /**
   * Get named KeyValue object.
   * 
   * @param key
   * @return named object or null if not existing
   */
  public KeyValue getKeyValue(String key) {
    if (n_values.containsKey(key)) {
      active.add(key);
    }
    return (KeyValue) n_values.get(key);
  }

  /**
   * Returns list of KeyValue objects that have been read or set.
   *
   * @return List containing KeyValue objects
   */
  public List getActive() {
    return active;
  }

  public String toString() {

    StringBuffer sb = new StringBuffer();

    sb.append("Original map {");

    Iterator i = o_values.keySet().iterator();

    while (i.hasNext()) {
      Object o = i.next();
      sb.append("[" + o.toString()).append(", ");
      sb.append(((String[]) o_values.get(o))[0]).append("] ");
    }
    sb.append("}\n");

    sb.append("Used map {");

    i = n_values.keySet().iterator();

    while (i.hasNext()) {
      Object o = i.next();
      sb.append("[" + o.toString()).append(", ");
      sb.append(n_values.get(o)).append("] ");
    }
    sb.append("}\n");

    return sb.toString();
  }

  /**
   * Returns a string usable in urls that contains all active parameters and
   * their values, if submitted encoding does not work values are encoded with
   * utf-8 encoding.
   *
   * @param encoding
   * @return
   */
  public String getParameterString(String encoding)
      throws UnsupportedEncodingException {

    boolean useEncoding = true;
    StringBuffer sb = new StringBuffer();

    try {
      URLEncoder.encode("", encoding);
    } catch (UnsupportedEncodingException e) {
      useEncoding = false;
    }

    List l = getActive();

    Iterator i = l.iterator();
    while (i.hasNext()) {
      KeyValue kv = (KeyValue) i.next();
      if (useEncoding) {
        sb.append(URLEncoder.encode(kv.getKey(), encoding)).append("=").append(
            URLEncoder.encode(kv.getValue(), encoding));
      } else {
        sb.append(URLEncoder.encode(kv.getKey(), "utf-8")).append("=").append(
            URLEncoder.encode(kv.getValue(), "utf-8"));
      }
      if (i.hasNext()) {
        sb.append("&");
      }
    }
    return sb.toString();
  }

  public Object clone(){
    SearchForm newForm=new SearchForm(new HashMap());
    newForm.active.addAll(active);
    newForm.o_values.putAll(o_values);
    newForm.n_values.putAll(n_values);
    return newForm;
  }

}
