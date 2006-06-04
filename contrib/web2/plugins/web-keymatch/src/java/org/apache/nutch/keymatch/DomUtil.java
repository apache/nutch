/*
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.keymatch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.xerces.parsers.DOMParser;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class DomUtil {

  /**
   * Returns parsed dom tree or null if any error
   * 
   * @param is
   * @return
   */
  public static Element getDom(InputStream is) {

    Element element = null;

    DOMParser parser = new DOMParser();

    InputSource input;
    try {
      input = new InputSource(is);
      input.setEncoding("UTF-8");
      parser.parse(input);
      element = (Element) parser.getDocument().getChildNodes().item(0);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (SAXException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return element;
  }

  /**
   * save dom into ouputstream
   * 
   * @param os
   * @param e
   */
  public static void saveDom(OutputStream os, Element e) {

    DOMSource source = new DOMSource(e);
    TransformerFactory transFactory = TransformerFactory.newInstance();
    Transformer transformer;
    try {
      transformer = transFactory.newTransformer();
      transformer.setOutputProperty("indent", "yes");
      StreamResult result = new StreamResult(os);
      transformer.transform(source, result);
      os.flush();
    } catch (UnsupportedEncodingException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (TransformerConfigurationException e2) {
      // TODO Auto-generated catch block
      e2.printStackTrace();
    } catch (TransformerException ex) {
      // TODO Auto-generated catch block
      ex.printStackTrace();
    }
  }
}
