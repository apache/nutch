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
package org.apache.nutch.indexer.solr;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.ObjectCache;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class SolrMappingReader {
  public static Logger LOG = LoggerFactory.getLogger(SolrMappingReader.class);
  
  private Configuration conf;
  
  private Map<String, String> keyMap = new HashMap<String, String>();
  private Map<String, String> copyMap = new HashMap<String, String>();
  private String uniqueKey = "id";
  
  public static synchronized SolrMappingReader getInstance(Configuration conf) {
    ObjectCache cache = ObjectCache.get(conf);
    SolrMappingReader instance = (SolrMappingReader)cache.getObject(SolrMappingReader.class.getName());
    if (instance == null) {
      instance = new SolrMappingReader(conf);
      cache.setObject(SolrMappingReader.class.getName(), instance);
    }
    return instance;
  }

  protected SolrMappingReader(Configuration conf) {
    this.conf = conf;
    parseMapping();
  }

  private void parseMapping() {    
    InputStream ssInputStream = null;
    ssInputStream = conf.getConfResourceAsInputStream(conf.get(SolrConstants.MAPPING_FILE, "solrindex-mapping.xml"));

    InputSource inputSource = new InputSource(ssInputStream);
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document document = builder.parse(inputSource);
      Element rootElement = document.getDocumentElement();
      NodeList fieldList = rootElement.getElementsByTagName("field");
      if (fieldList.getLength() > 0) {
        for (int i = 0; i < fieldList.getLength(); i++) {
          Element element = (Element) fieldList.item(i);
          LOG.info("source: " + element.getAttribute("source") + " dest: " + element.getAttribute("dest"));
          keyMap.put(element.getAttribute("source"), element.getAttribute("dest"));
        }
      }
      NodeList copyFieldList = rootElement.getElementsByTagName("copyField");
      if (copyFieldList.getLength() > 0) {
        for (int i = 0; i < copyFieldList.getLength(); i++) {
          Element element = (Element) copyFieldList.item(i);
          LOG.info("source: " + element.getAttribute("source") + " dest: " + element.getAttribute("dest"));
          copyMap.put(element.getAttribute("source"), element.getAttribute("dest"));
        }
      }
      NodeList uniqueKeyItem = rootElement.getElementsByTagName("uniqueKey");
      if (uniqueKeyItem.getLength() > 1) {
        LOG.warn("More than one unique key definitions found in solr index mapping, using default 'id'");
        uniqueKey = "id";
      }
      else if (uniqueKeyItem.getLength() == 0) {
        LOG.warn("No unique key definition found in solr index mapping using, default 'id'");
      }
      else{
    	  uniqueKey = uniqueKeyItem.item(0).getFirstChild().getNodeValue();
      }
    } catch (MalformedURLException e) {
        LOG.warn(e.toString());
    } catch (SAXException e) {
        LOG.warn(e.toString());
    } catch (IOException e) {
    	LOG.warn(e.toString());
    } catch (ParserConfigurationException e) {
    	LOG.warn(e.toString());
    } 
  }
	  
  public Map<String, String> getKeyMap() {
    return keyMap;
  }
	  
  public Map<String, String> getCopyMap() {
    return copyMap;
  }
	  
  public String getUniqueKey() {
    return uniqueKey;
  }

  public String hasCopy(String key) {
    if (copyMap.containsKey(key)) {
      key = copyMap.get(key);
    }
    return key;
  }

  public String mapKey(String key) throws IOException {
    if(keyMap.containsKey(key)) {
      key = keyMap.get(key);
    }
    return key;
  }

  public String mapCopyKey(String key) throws IOException {
    if(copyMap.containsKey(key)) {
      key = copyMap.get(key);
    }
    return key;
  }
}
