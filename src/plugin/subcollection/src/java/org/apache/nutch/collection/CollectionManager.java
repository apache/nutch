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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.util.DomUtil;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.ObjectCache;
import org.apache.xerces.dom.DocumentImpl;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class CollectionManager extends Configured {

  public static final String DEFAULT_FILE_NAME = "subcollections.xml";

  static final Log LOG = LogFactory.getLog(CollectionManager.class);

  transient Map collectionMap = new HashMap();

  transient URL configfile;
  
  public CollectionManager(Configuration conf) {
    super(conf);
    init();
  }
  
  /** 
   * Used for testing
   */
  protected CollectionManager(){
    super(NutchConfiguration.create());
  }

  protected void init(){
    try {
      if (LOG.isInfoEnabled()) { LOG.info("initializing CollectionManager"); }
      // initialize known subcollections
      configfile = getConf().getResource(
          getConf().get("subcollections.config", DEFAULT_FILE_NAME));

      InputStream input = getConf().getConfResourceAsInputStream(
          getConf().get("subcollections.config", DEFAULT_FILE_NAME));
      parse(input);
    } catch (Exception e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Error occured:" + e);
        e.printStackTrace(LogUtil.getWarnStream(LOG));
      }
    }
  }

  protected void parse(InputStream input) {
    Element collections = DomUtil.getDom(input);

    if (collections != null) {
      NodeList nodeList = collections
          .getElementsByTagName(Subcollection.TAG_COLLECTION);

      if (LOG.isInfoEnabled()) {
        LOG.info("file has" + nodeList.getLength() + " elements");
      }
      
      for (int i = 0; i < nodeList.getLength(); i++) {
        Element scElem = (Element) nodeList.item(i);
        Subcollection subCol = new Subcollection(getConf());
        subCol.initialize(scElem);
        collectionMap.put(subCol.name, subCol);
      }
    } else if (LOG.isInfoEnabled()) {
      LOG.info("Cannot find collections");
    }
  }
  
  public static CollectionManager getCollectionManager(Configuration conf) {
    String key = "collectionmanager";
    ObjectCache objectCache = ObjectCache.get(conf);
    CollectionManager impl = (CollectionManager)objectCache.getObject(key);
    if (impl == null) {
      try {
        if (LOG.isInfoEnabled()) {
          LOG.info("Instantiating CollectionManager");
        }
        impl=new CollectionManager(conf);
        objectCache.setObject(key,impl);
      } catch (Exception e) {
        throw new RuntimeException("Couldn't create CollectionManager",e);
      }
    }
    return impl;
  }

  /**
   * Returns named subcollection
   * 
   * @param id
   * @return Named SubCollection (or null if not existing)
   */
  public Subcollection getSubColection(final String id) {
    return (Subcollection) collectionMap.get(id);
  }

  /**
   * Delete named subcollection
   * 
   * @param id
   *          Id of SubCollection to delete
   */
  public void deleteSubCollection(final String id) throws IOException {
    final Subcollection subCol = getSubColection(id);
    if (subCol != null) {
      collectionMap.remove(id);
    }
  }

  /**
   * Create a new subcollection.
   * 
   * @param name
   *          Name of SubCollection to create
   * @return Created SubCollection or null if allready existed
   */
  public Subcollection createSubCollection(final String id, final String name) {
    Subcollection subCol = null;

    if (!collectionMap.containsKey(id)) {
      subCol = new Subcollection(id, name, getConf());
      collectionMap.put(id, subCol);
    }

    return subCol;
  }

  /**
   * Return names of collections url is part of
   * 
   * @param url
   *          The url to test against Collections
   * @return Space delimited string of collection names url is part of
   */
  public String getSubCollections(final String url) {
    String collections = "";
    final Iterator iterator = collectionMap.values().iterator();

    while (iterator.hasNext()) {
      final Subcollection subCol = (Subcollection) iterator.next();
      if (subCol.filter(url) != null) {
        collections += " " + subCol.name;
      }
    }
    if (LOG.isTraceEnabled()) { LOG.trace("subcollections:" + collections); }
    
    return collections;
  }

  /**
   * Returns all collections
   * 
   * @return All collections CollectionManager knows about
   */
  public Collection getAll() {
    return collectionMap.values();
  }

  /**
   * Save collections into file
   * 
   * @throws Exception
   */
  public void save() throws IOException {
    try {
      final FileOutputStream fos = new FileOutputStream(new File(configfile
          .getFile()));
      final Document doc = new DocumentImpl();
      final Element collections = doc
          .createElement(Subcollection.TAG_COLLECTIONS);
      final Iterator iterator = collectionMap.values().iterator();

      while (iterator.hasNext()) {
        final Subcollection subCol = (Subcollection) iterator.next();
        final Element collection = doc
            .createElement(Subcollection.TAG_COLLECTION);
        collections.appendChild(collection);
        final Element name = doc.createElement(Subcollection.TAG_NAME);
        name.setNodeValue(subCol.getName());
        collection.appendChild(name);
        final Element whiteList = doc
            .createElement(Subcollection.TAG_WHITELIST);
        whiteList.setNodeValue(subCol.getWhiteListString());
        collection.appendChild(whiteList);
        final Element blackList = doc
            .createElement(Subcollection.TAG_BLACKLIST);
        blackList.setNodeValue(subCol.getBlackListString());
        collection.appendChild(blackList);
      }

      DomUtil.saveDom(fos, collections);
      fos.flush();
      fos.close();
    } catch (FileNotFoundException e) {
      throw new IOException(e.toString());
    }
  }
}
