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
package org.apache.nutch.collection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.net.URLFilter;
import org.apache.xerces.util.DOMUtil;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * SubCollection represents a subset of index, you can define url patterns that
 * will indicate that particular page (url) is part of SubCollection.
 */
public class Subcollection extends Configured implements URLFilter {

  public static final String TAG_COLLECTIONS = "subcollections";
  public static final String TAG_COLLECTION = "subcollection";
  public static final String TAG_WHITELIST = "whitelist";
  public static final String TAG_BLACKLIST = "blacklist";
  public static final String TAG_NAME = "name";
  public static final String TAG_KEY = "key";
  public static final String TAG_ID = "id";

  List<String> blackList = new ArrayList<String>();
  List<String> whiteList = new ArrayList<String>();

  /**
   * SubCollection identifier
   */
  String id;

  /**
   * SubCollection key
   */
  String key;

  /**
   * SubCollection name
   */
  String name;

  /**
   * SubCollection whitelist as String
   */
  String wlString;

  /**
   * SubCollection blacklist as String
   */
  String blString;
  
  /**
   * Whether the white and black lists are case sensitive
   */
  boolean caseInsensitive = false;

  /**
   * public Constructor
   * 
   * @param id
   *          Id of SubCollection
   * @param name
   *          Name of SubCollection
   * @param conf A populated {@link Configuration}
   */
  public Subcollection(String id, String name, Configuration conf) {
    this(id, name, null, conf);
  }

  /**
   * public Constructor
   * 
   * @param id
   *          Id of SubCollection
   * @param name
   *          Name of SubCollection
   * @param key SubCollection key
   * @param conf A populated {@link Configuration}
   */
  public Subcollection(String id, String name, String key, Configuration conf) {
    this(conf);
    this.id = id;
    this.key = key;
    this.name = name;
    caseInsensitive = conf.getBoolean("subcollection.case.insensitive", false);
  }

  public Subcollection(Configuration conf) {
    super(conf);
    caseInsensitive = conf.getBoolean("subcollection.case.insensitive", false);
  }

  /**
   * @return Returns the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return Returns the key
   */
  public String getKey() {
    return key;
  }

  /**
   * @return Returns the id
   */
  public String getId() {
    return id;
  }

  /**
   * Returns whitelist
   * 
   * @return Whitelist entries
   */
  public List<String> getWhiteList() {
    return whiteList;
  }

  /**
   * Returns whitelist String
   * 
   * @return Whitelist String
   */
  public String getWhiteListString() {
    return wlString;
  }

  /**
   * Returns blacklist String
   * 
   * @return Blacklist String
   */
  public String getBlackListString() {
    return blString;
  }

  /**
   * @param whiteList
   *          The whiteList to set.
   */
  public void setWhiteList(ArrayList<String> whiteList) {
    this.whiteList = whiteList;
  }

  /**
   * Simple "indexOf" currentFilter for matching patterns.
   * 
   * <pre>
   *  rules for evaluation are as follows:
   *  1. if pattern matches in blacklist then url is rejected
   *  2. if pattern matches in whitelist then url is allowed
   *  3. url is rejected
   * </pre>
   * 
   * @see org.apache.nutch.net.URLFilter#filter(java.lang.String)
   */
  public String filter(String urlString) {
    // first the blacklist
    Iterator<String> i = blackList.iterator();
    while (i.hasNext()) {
      String row = (String) i.next();
      if (urlString.contains(row))
        return null;
    }

    // then whitelist
    i = whiteList.iterator();
    while (i.hasNext()) {
      String row = (String) i.next();
      if (urlString.contains(row))
        return urlString;
    }
    return null;
  }

  /**
   * Initialize Subcollection from dom element
   * 
   * @param collection A DOM {@link org.w3c.dom.Element} for use 
   * in creating the {@link Subcollection}
   */
  public void initialize(Element collection) {
    this.id = DOMUtil.getChildText(
        collection.getElementsByTagName(TAG_ID).item(0)).trim();
    this.name = DOMUtil.getChildText(
        collection.getElementsByTagName(TAG_NAME).item(0)).trim();
    this.wlString = DOMUtil.getChildText(
        collection.getElementsByTagName(TAG_WHITELIST).item(0)).trim();

    parseList(this.whiteList, wlString);

    // Check if there's a blacklist we need to parse
    NodeList nodeList = collection.getElementsByTagName(TAG_BLACKLIST);
    if (nodeList.getLength() > 0) {
      this.blString = DOMUtil.getChildText(nodeList.item(0)).trim();
      parseList(this.blackList, blString);
    }

    // Check if there's a key element or set default name
    nodeList = collection.getElementsByTagName(TAG_KEY);
    if (nodeList.getLength() == 1) {
      this.key = DOMUtil.getChildText(nodeList.item(0)).trim();
    }
  }

  /**
   * Create a list of patterns from a chunk of text, patterns are separated 
   * with a newline
   * 
   * @param list An initialized {@link List} to insert String patterns.
   * @param text A chunkl fo text (hopefully) containing patterns.
   */
  protected void parseList(List<String> list, String text) {
    list.clear();

    StringTokenizer st = new StringTokenizer(text, "\n\r");

    while (st.hasMoreElements()) {
      String line = (String) st.nextElement();
      line = line.trim();
      if (line.isEmpty())
        continue;
      if (caseInsensitive) {
        line = line.toLowerCase(Locale.ROOT);
      }
      list.add(line);
    }
  }

  /**
   * Set contents of blacklist from String
   * 
   * @param list
   *          the blacklist contents
   */
  public void setBlackList(String list) {
    this.blString = list;
    parseList(blackList, list);
  }

  /**
   * Set contents of whitelist from String
   * 
   * @param list
   *          the whitelist contents
   */
  public void setWhiteList(String list) {
    this.wlString = list;
    parseList(whiteList, list);
  }
}
