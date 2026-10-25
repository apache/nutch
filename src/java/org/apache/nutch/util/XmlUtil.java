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
package org.apache.nutch.util;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;

import org.apache.xerces.parsers.DOMParser;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;

/**
 * Helpers that return XML parser/transformer factories configured to reject
 * XXE (external entity) attacks. Prefer these over bare
 * {@link DocumentBuilderFactory#newInstance()} /
 * {@link SAXParserFactory#newInstance()} /
 * {@link TransformerFactory#newInstance()}.
 */
public final class XmlUtil {

  private static final String DISALLOW_DOCTYPE_DECL =
      "http://apache.org/xml/features/disallow-doctype-decl";
  private static final String EXTERNAL_GENERAL_ENTITIES =
      "http://xml.org/sax/features/external-general-entities";
  private static final String EXTERNAL_PARAMETER_ENTITIES =
      "http://xml.org/sax/features/external-parameter-entities";
  private static final String LOAD_EXTERNAL_DTD =
      "http://apache.org/xml/features/nonvalidating/load-external-dtd";

  private XmlUtil() {
  }

  /**
   * @return a {@link DocumentBuilderFactory} hardened against XXE
   * @throws ParserConfigurationException if a required secure feature is
   *           unsupported
   */
  public static DocumentBuilderFactory newSecureDocumentBuilderFactory()
      throws ParserConfigurationException {
    return newSecureDocumentBuilderFactory(false);
  }

  /**
   * @param namespaceAware whether the factory should be namespace-aware
   * @return a {@link DocumentBuilderFactory} hardened against XXE
   * @throws ParserConfigurationException if a required secure feature is
   *           unsupported
   */
  public static DocumentBuilderFactory newSecureDocumentBuilderFactory(
      boolean namespaceAware) throws ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(namespaceAware);
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setFeature(DISALLOW_DOCTYPE_DECL, true);
    factory.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
    factory.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
    factory.setFeature(LOAD_EXTERNAL_DTD, false);
    factory.setXIncludeAware(false);
    // Sonar java:S2755 "Going the extra mile" — does not alone block XXE
    factory.setExpandEntityReferences(false);
    return factory;
  }

  /**
   * @return a {@link SAXParserFactory} hardened against XXE
   * @throws ParserConfigurationException if a required secure feature is
   *           unsupported
   * @throws SAXNotRecognizedException if a feature name is not recognized
   * @throws SAXNotSupportedException if a feature value is not supported
   */
  public static SAXParserFactory newSecureSAXParserFactory()
      throws ParserConfigurationException, SAXNotRecognizedException,
      SAXNotSupportedException {
    SAXParserFactory factory = SAXParserFactory.newInstance();
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setFeature(DISALLOW_DOCTYPE_DECL, true);
    factory.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
    factory.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
    factory.setXIncludeAware(false);
    return factory;
  }

  /**
   * @return a {@link TransformerFactory} that cannot load external DTDs or
   *         stylesheets
   * @throws TransformerConfigurationException if secure attributes cannot be
   *           applied
   */
  public static TransformerFactory newSecureTransformerFactory()
      throws TransformerConfigurationException {
    TransformerFactory factory = TransformerFactory.newInstance();
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
    return factory;
  }

  /**
   * Apply XXE-hardening features to an Xerces {@link DOMParser}.
   *
   * @param parser parser to configure
   * @throws SAXException if a secure feature cannot be set
   */
  public static void configureSecure(DOMParser parser) throws SAXException {
    parser.setFeature(DISALLOW_DOCTYPE_DECL, true);
    parser.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
    parser.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
    parser.setFeature(LOAD_EXTERNAL_DTD, false);
    try {
      parser.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    } catch (SAXNotRecognizedException | SAXNotSupportedException e) {
      // Xerces' DOMParser does not expose the JAXP secure-processing feature.
      // DTDs and external entities are already disabled above.
    }
  }
}
