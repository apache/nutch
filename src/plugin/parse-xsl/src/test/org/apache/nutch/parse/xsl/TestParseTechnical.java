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
package org.apache.nutch.parse.xsl;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileReader;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.junit.Test;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 *  
 */
public class TestParseTechnical extends AbstractCrawlTest {

  /**
   * Executes some xpath on neko parsed document
   */
  @Test
  public void testXpathNeko() {
    try {
      DocumentFragment doc = parseNeko(new InputSource(
          new FileReader(new File(sampleDir, "sample1/book1.html"))));
      XPath xpath = XPathFactory.newInstance().newXPath();
      NodeList result = (NodeList) xpath.compile("//DIV").evaluate(doc,
          XPathConstants.NODESET);
      assertNotNull(result);
      assertEquals(3, result.getLength());
      System.out.println(result.getLength());
      result = (NodeList) xpath.compile("//HTML").evaluate(doc,
          XPathConstants.NODESET);
      assertNotNull(result);
      System.out.println(result.getLength());
      assertEquals(1, result.getLength());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Executes some xpath on TagSoup parsed document
   * TODO not working with TagSoup. Investigate why.
   */
  @Test
  public void testXpathTagSoup() {
    try {
      DocumentFragment doc = parseTagSoup(new InputSource(
          new FileReader(new File(sampleDir, "sample1/book1.html"))));
      XPath xpath = XPathFactory.newInstance().newXPath();
      NodeList result = (NodeList) xpath.compile("//div").evaluate(doc,
          XPathConstants.NODESET);
      assertNotNull(result);
      assertEquals(3, result.getLength());
      System.out.println(result.getLength());
      result = (NodeList) xpath.compile("//html").evaluate(doc,
          XPathConstants.NODESET);
      assertNotNull(result);
      System.out.println(result.getLength());
      assertEquals(1, result.getLength());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
