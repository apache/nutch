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
package org.apache.nutch.core.jsoup.extractor;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.core.jsoup.extractor.JsoupDocument.DocumentField;
import org.apache.nutch.core.jsoup.extractor.normalizer.Normalizable;
import org.apache.nutch.util.ObjectCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class JsoupDocumentReader {
  
  public static Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  
  private Configuration conf;

  private List<JsoupDocument> jsoupDocuments = new ArrayList<JsoupDocument>();
  private Map<String, Normalizable> normalizerMap = new HashMap<>();

  public static synchronized JsoupDocumentReader getInstance(Configuration conf) {
    ObjectCache cache = ObjectCache.get(conf);
    JsoupDocumentReader instance = (JsoupDocumentReader) cache
        .getObject(JsoupDocumentReader.class.getName());
    if (instance == null) {
      instance = new JsoupDocumentReader(conf);
      cache.setObject(JsoupDocumentReader.class.getName(), instance);
    }
    return instance;
  }

  private void parse() {
    InputStream inputStream = null;
    inputStream = conf.getConfResourceAsInputStream(
        conf.get(JsoupExtractorConstants.JSOUP_DOC_PROPERTY_FILE, ""));
    InputSource inputSource = new InputSource(inputStream);
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document xmlDocument = builder.parse(inputSource);
      Element rootElement = xmlDocument.getDocumentElement();

      parseNormalizers(rootElement);
      parseDocuments(rootElement);

    } catch (ParserConfigurationException ex) {
      LOG.error("{}", ex.toString());
    } catch (SAXException ex) {
      LOG.error("{}", ex.toString());
    } catch (IOException ex) {
      LOG.error("{}", ex.toString());
    }

  }

  private void parseNormalizers(Element rootElement) {
    Element parentElement = (Element) rootElement
        .getElementsByTagName(JsoupExtractorConstants.TAG_NORMALIZER_LIST)
        .item(0);
    NodeList nodelist = parentElement
        .getElementsByTagName(JsoupExtractorConstants.TAG_NORMALIZER);
    for (int i = 0; i < nodelist.getLength(); i++) {
      Element node = (Element) nodelist.item(i);
      String name = node.getAttribute(JsoupExtractorConstants.ATTR_NAME);
      String className = node.getAttribute(JsoupExtractorConstants.ATTR_CLASS);
      try {
        Class<?> clazz = Class.forName(className);
        Normalizable normalizable = (Normalizable) Class
            .forName(Normalizable.class.getName()).cast(clazz);
        normalizerMap.put(name, normalizable);
        LOG.info("Normalizer name: {}, class: {}", name, normalizable.getClass().getName());
      } catch (ClassNotFoundException ex) {
        LOG.warn(
            "Invalid class attribute for Normalizer: Class may not implement Normalizable interface: {}",
            ex.toString());
        continue;
      }
    }
  }

  private void parseDocuments(Element rootElement) {
    NodeList documentList = rootElement
        .getElementsByTagName(JsoupExtractorConstants.TAG_DOCUMENT);
    for (int i = 0; i < documentList.getLength(); i++) {
      Element document = (Element) documentList.item(i);
      String urlRegex = document
          .getAttribute(JsoupExtractorConstants.ATTR_URL_PATTERN);
      Pattern urlPattern = Pattern.compile(urlRegex);
      JsoupDocument jsoupDocument = new JsoupDocument(urlPattern);
      NodeList fieldList = document
          .getElementsByTagName(JsoupExtractorConstants.TAG_FIELD);

      for (int j = 0; j < fieldList.getLength(); j++) {
        Element field = (Element) fieldList.item(j);
        String name = field.getAttribute(JsoupExtractorConstants.ATTR_NAME);
        NodeList fieldCssSelectors = field
            .getElementsByTagName(JsoupExtractorConstants.TAG_CSS_SELECTOR);
        NodeList fieldAttrs = field
            .getElementsByTagName(JsoupExtractorConstants.TAG_ATTRIBUTE);
        NodeList fieldDefaultValues = field
            .getElementsByTagName(JsoupExtractorConstants.TAG_DEFAULT_VALUE);
        String cssSelector = "";
        if (fieldCssSelectors.getLength() > 0) {
          cssSelector = fieldCssSelectors.item(0).getTextContent();
        }
        DocumentField documentField = new DocumentField(name, cssSelector);
        if (fieldAttrs.getLength() > 0) {
          String attr = fieldAttrs.item(0).getTextContent();
          documentField.setAttribute(attr);
        }
        String defaultValue = "";
        if (fieldDefaultValues.getLength() > 0) {
          defaultValue = fieldDefaultValues.item(0).getTextContent();
        }
        documentField.setDefaultValue(defaultValue);
        NodeList fieldNormalizers = field
            .getElementsByTagName(JsoupExtractorConstants.TAG_NORMALIZER);
        String normalizerName = "";
        if (fieldNormalizers.getLength() > 0) {
          normalizerName = fieldNormalizers.item(0).getTextContent();
          if (normalizerMap.containsKey(normalizerName)) {
            documentField.setNormalizer(normalizerMap.get(normalizerName));
          }
        }
        jsoupDocument.addField(documentField);
      }
      jsoupDocuments.add(jsoupDocument);
      LOG.info("{}", jsoupDocument.toString());
    }
  }

  protected JsoupDocumentReader(Configuration conf) {
    this.conf = conf;
    parse();
  }

  public List<JsoupDocument> getDocuments() {
    return jsoupDocuments;
  }
}
