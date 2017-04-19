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
package org.apache.nutch.indexwriter.hbase;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.ObjectCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class HBaseMappingReader {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private Configuration conf;

  private Map<String, String> keyMap = new HashMap<String, String>();
  private Map<String, String> familyMap = new HashMap<String, String>();

  private String rowKey = HBaseConstants.DEFAULT_ROW_KEY;
  private String tableName = HBaseConstants.DEFAULT_TABLE_NAME;

  public static synchronized HBaseMappingReader getInstance(Configuration conf) {
    ObjectCache cache = ObjectCache.get(conf);
    HBaseMappingReader instance = (HBaseMappingReader) cache.getObject(HBaseMappingReader.class.getName());
    if (instance == null) {
      instance = new HBaseMappingReader(conf);
      cache.setObject(HBaseMappingReader.class.getName(), instance);
    }
    return instance;
  }

  protected HBaseMappingReader(Configuration conf) {
    this.conf = conf;
    parseMapping();
  }

  private void parseMapping() {
    InputStream inputStream = conf
        .getConfResourceAsInputStream(conf.get(HBaseConstants.HBASE_MAPPING_FILE, HBaseConstants.DEFAULT_MAPPING_FILE));
    InputSource inputSource = new InputSource(inputStream);

    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document document = builder.parse(inputSource);
      Element rootElement = document.getDocumentElement();

      NodeList tableItems = rootElement.getElementsByTagName(HBaseConstants.TAG_TABLE);
      if (tableItems.getLength() == 0) {
        LOG.warn("No table definition found in hbase index mapping, using default table name: '"
            + HBaseConstants.DEFAULT_ROW_KEY + "'");
      } else if (tableItems.getLength() > 1) {
        LOG.warn("More than one table definition found in hbase index mapping, using default table name: '"
            + HBaseConstants.DEFAULT_ROW_KEY + "'");
      } else {
        Element tableElement = (Element) tableItems.item(0);
        tableName = tableElement.getAttribute(HBaseConstants.ATTR_NAME);
        if (tableName.isEmpty()) {
          LOG.warn("Table name not found/empty in hbase index mapping, using default table name: '"
              + HBaseConstants.DEFAULT_ROW_KEY + "'");
          tableName = HBaseConstants.DEFAULT_TABLE_NAME;
        }
      }

      NodeList fieldItems = rootElement.getElementsByTagName(HBaseConstants.TAG_FIELD);
      for (int i = 0; i < fieldItems.getLength(); i++) {
        Element fieldElement = (Element) fieldItems.item(i);
        String src = fieldElement.getAttribute(HBaseConstants.ATTR_SRC);
        String columnFamily = fieldElement.getAttribute(HBaseConstants.ATTR_FAMILY);
        String qualifier = fieldElement.getAttribute(HBaseConstants.ATTR_QUALIFIER);
        LOG.info("Field source: " + src + ", dest: " + fieldElement.getAttribute(HBaseConstants.ATTR_DEST)
            + ", column-family: " + columnFamily + ", qualifier: " + qualifier);

        keyMap.put(src, qualifier.isEmpty() ? fieldElement.getAttribute(HBaseConstants.ATTR_DEST) : qualifier);
        if (!columnFamily.isEmpty()) {
          familyMap.put(src, columnFamily);
        }
      }

      NodeList rowItems = rootElement.getElementsByTagName(HBaseConstants.TAG_ROW);
      if (rowItems.getLength() > 1) {
        LOG.warn("More than one row key definitions found in hbase index mapping, using default configuration '"
            + HBaseConstants.DEFAULT_ROW_KEY + "'");
      } else if (rowItems.getLength() == 0) {
        LOG.warn("No row key definitions found in hbase index mapping, using default configuration '"
            + HBaseConstants.DEFAULT_ROW_KEY + "'");
      } else {
        rowKey = ((Element) rowItems.item(0)).getTextContent();
        if (rowKey.isEmpty()) {
          LOG.warn("Row key is not found/empty in hbase index mapping, using default configuration '"
              + HBaseConstants.DEFAULT_ROW_KEY + "'");
          rowKey = HBaseConstants.DEFAULT_ROW_KEY;
        }
      }

    } catch (SAXException ex) {
      LOG.warn(ex.toString());
    } catch (IOException ex) {
      LOG.warn(ex.toString());
    } catch (ParserConfigurationException ex) {
      LOG.warn(ex.toString());
    }
  }

  public String getRowKey() {
    return rowKey;
  }

  public String getTableName() {
    return tableName;
  }

  public String getMappedKey(String key) {
    if (keyMap.containsKey(key)) {
      key = keyMap.get(key);
    }
    return key;
  }

  public String getFamily(String key) {
    if (familyMap.containsKey(key)) {
      return familyMap.get(key);
    }
    return HBaseConstants.DEFAULT_COLUMN_FAMILY;
  }

}
