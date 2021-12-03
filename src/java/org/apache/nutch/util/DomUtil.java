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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.xerces.parsers.DOMParser;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DomUtil {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Returns parsed dom tree or null if any error
   * 
   * @param is XML {@link InputStream}
   * @return A parsed DOM tree from the given {@link InputStream}.
   */
  public static Element getDom(InputStream is) {

    Element element = null;

    DOMParser parser = new DOMParser();

    InputSource input;
    try {
      input = new InputSource(is);
      input.setEncoding("UTF-8");
      parser.parse(input);
      int i = 0;
      while (!(parser.getDocument().getChildNodes().item(i) instanceof Element)) {
        i++;
      }
      element = (Element) parser.getDocument().getChildNodes().item(i);
    } catch (FileNotFoundException e) {
      LOG.error("Error: ", e);
    } catch (SAXException e) {
      LOG.error("Error: ", e);
    } catch (IOException e) {
      LOG.error("Error: ", e);
    }
    return element;
  }

  /**
   * Save dom into {@link OutputStream}
   * 
   * @param os Output DOM XML stream to save to
   * @param e A specific DOM {@link org.w3c.dom.Element} to save
   */
  public static void saveDom(OutputStream os, Element e) {

    DOMSource source = new DOMSource(e);
    TransformerFactory transFactory = TransformerFactory.newInstance();
    Transformer transformer;
    try {
      transformer = transFactory.newTransformer();
      transformer.setOutputProperty("indent", "yes");
      transformer.setOutputProperty(OutputKeys.ENCODING,
          StandardCharsets.UTF_8.name());
      StreamResult result = new StreamResult(os);
      transformer.transform(source, result);
      os.flush();
    } catch (IOException | TransformerException ex) {
      LOG.error("Error: ", ex);
    }
  }

  /**
   * Save dom into {@link OutputStream}
   * 
   * @param os Output DOM XML stream to save to
   * @param doc A specific DOM {@link org.w3c.dom.DocumentFragment} to save
   */
  public static void saveDom(OutputStream os, DocumentFragment doc) {
    NodeList docChildren = doc.getChildNodes();
    for (int i = 0; i < docChildren.getLength(); i++) {
      Node child = docChildren.item(i);
      if (child instanceof Element) {
        saveDom(os, (Element) child);
      } else {
        try {
          os.write(child.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException ex) {
          LOG.error("Error: ", ex);
        }
      }
    }
  }
}
