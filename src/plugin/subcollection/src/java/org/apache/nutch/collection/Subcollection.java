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
public class Subcollection extends Configured implements URLFilter{
  
  public static final String TAG_COLLECTIONS="subcollections";
  public static final String TAG_COLLECTION="subcollection";
  public static final String TAG_WHITELIST="whitelist";
  public static final String TAG_BLACKLIST="blacklist";
  public static final String TAG_NAME="name";
  public static final String TAG_ID="id";

  ArrayList<String> blackList = new ArrayList<String>();

  ArrayList<String> whiteList = new ArrayList<String>();

  /** 
   * SubCollection identifier
   */
  String id;

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

  /** public Constructor
   * 
   * @param id id of SubCollection
   * @param name name of SubCollection
   */
  public Subcollection(String id, String name, Configuration conf) {
    this(conf);
    this.id=id;
    this.name = name;
  }

  public Subcollection(Configuration conf){
    super(conf);
  }
  
  /**
   * @return Returns the name
   */
  public String getName() {
    return name;
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
  public ArrayList<String> getWhiteList() {
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
      String row = i.next();
      if (urlString.indexOf(row) != -1)
        return null;
    }

    // then whitelist
    i = whiteList.iterator();
    while (i.hasNext()) {
      String row = i.next();
      if (urlString.indexOf(row) != -1)
        return urlString;
    }
    return null;
  }

  /**
   * Initialize Subcollection from dom element
   * 
   * @param collection
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
  }

  /**
   * Create a list of patterns from chunk of text, patterns are separated with
   * newline
   * 
   * @param list
   * @param text
   */
  protected void parseList(ArrayList<String> list, String text) {
    list.clear();

    StringTokenizer st = new StringTokenizer(text, "\n\r");

    while (st.hasMoreElements()) {
      String line = (String) st.nextElement();
      list.add(line.trim());
    }
  }

  /**
   * Set contents of blacklist from String
   * 
   * @param list the blacklist contents
   */
  public void setBlackList(String list) {
    this.blString = list;
    parseList(blackList, list);
  }

  /**
   * Set contents of whitelist from String
   * 
   * @param list the whitelist contents
   */
  public void setWhiteList(String list) {
    this.wlString = list;
    parseList(whiteList, list);
  }
}
