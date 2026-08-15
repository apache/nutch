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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link XmlUtil} XXE hardening.
 */
public class TestXmlUtil {

  @Test
  public void testSecureFactoryParsesSimpleDocument() throws Exception {
    DocumentBuilderFactory factory = XmlUtil.newSecureDocumentBuilderFactory();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(new ByteArrayInputStream(
        "<root attr=\"ok\"><child/></root>".getBytes(StandardCharsets.UTF_8)));
    assertEquals("root", doc.getDocumentElement().getTagName());
    assertEquals("ok", doc.getDocumentElement().getAttribute("attr"));
  }

  @Test
  public void testSecureFactoryRejectsDoctype() throws Exception {
    DocumentBuilderFactory factory = XmlUtil.newSecureDocumentBuilderFactory();
    DocumentBuilder builder = factory.newDocumentBuilder();
    String xxe = "<?xml version=\"1.0\"?>"
        + "<!DOCTYPE foo [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>"
        + "<foo>&xxe;</foo>";
    assertThrows(SAXException.class, () -> builder
        .parse(new ByteArrayInputStream(xxe.getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  public void testDomUtilRejectsDoctype() {
    String xxe = "<?xml version=\"1.0\"?>"
        + "<!DOCTYPE foo [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>"
        + "<foo>&xxe;</foo>";
    Element element = DomUtil.getDom(
        new ByteArrayInputStream(xxe.getBytes(StandardCharsets.UTF_8)));
    // DomUtil swallows parse errors and returns null
    assertEquals(null, element);
  }

  @Test
  public void testNamespaceAwareFactory() throws Exception {
    DocumentBuilderFactory factory = XmlUtil
        .newSecureDocumentBuilderFactory(true);
    assertNotNull(factory.newDocumentBuilder());
  }
}
