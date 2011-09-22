/**
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

package org.apache.nutch.util.domain;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.util.domain.DomainSuffix.Status;
import org.apache.nutch.util.domain.TopLevelDomain.Type;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * For parsing xml files containing domain suffix definitions.
 * Parsed xml files should validate against 
 * <code>domain-suffixes.xsd</code>  
 * @author Enis Soztutar &lt;enis.soz.nutch@gmail.com&gt;
 */
class DomainSuffixesReader {

  private static final Logger LOG = LoggerFactory.getLogger(DomainSuffixesReader.class);

  void read(DomainSuffixes tldEntries, InputStream input) throws IOException{
    try {

      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setIgnoringComments(true);
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document document = builder.parse(new InputSource(input));

      Element root = document.getDocumentElement();
      
      if(root != null && root.getTagName().equals("domains")) {
        
        Element tlds = (Element)root.getElementsByTagName("tlds").item(0);
        Element suffixes = (Element)root.getElementsByTagName("suffixes").item(0);
        
        //read tlds
        readITLDs(tldEntries, (Element)tlds.getElementsByTagName("itlds").item(0));
        readGTLDs(tldEntries, (Element)tlds.getElementsByTagName("gtlds").item(0));
        readCCTLDs(tldEntries, (Element)tlds.getElementsByTagName("cctlds").item(0));
        
        readSuffixes(tldEntries, suffixes);
      }
      else {
        throw new IOException("xml file is not valid");
      }
    }
    catch (ParserConfigurationException ex) {
      LOG.warn(StringUtils.stringifyException(ex));
      throw new IOException(ex.getMessage());
    }
    catch (SAXException ex) {
      LOG.warn(StringUtils.stringifyException(ex));
      throw new IOException(ex.getMessage());
    }
  }

  void readITLDs(DomainSuffixes tldEntries, Element el) {
    NodeList children = el.getElementsByTagName("tld");
    for(int i=0;i<children.getLength();i++) {
      tldEntries.addDomainSuffix(readGTLD((Element)children.item(i), Type.INFRASTRUCTURE));
    }
  }
    
  void readGTLDs(DomainSuffixes tldEntries, Element el) {
    NodeList children = el.getElementsByTagName("tld");
    for(int i=0;i<children.getLength();i++) {
      tldEntries.addDomainSuffix(readGTLD((Element)children.item(i), Type.GENERIC));
    }
  }

  void readCCTLDs(DomainSuffixes tldEntries, Element el) throws IOException {
    NodeList children = el.getElementsByTagName("tld");
    for(int i=0;i<children.getLength();i++) {
      tldEntries.addDomainSuffix(readCCTLD((Element)children.item(i)));
    }
  }

  TopLevelDomain readGTLD(Element el, Type type) {
    String domain = el.getAttribute("domain");
    Status status = readStatus(el);
    float boost = readBoost(el);
    return new TopLevelDomain(domain, type, status, boost);
  }

  TopLevelDomain readCCTLD(Element el) throws IOException {
    String domain = el.getAttribute("domain");
    Status status = readStatus(el);
    float boost = readBoost(el);
    String countryName = readCountryName(el); 
    return new TopLevelDomain(domain, status, boost, countryName);  
  }
  
  /** read optional field status */
  Status readStatus(Element el) {
    NodeList list = el.getElementsByTagName("status");
    if(list == null || list.getLength() == 0)
      return DomainSuffix.DEFAULT_STATUS;
    return Status.valueOf(list.item(0).getFirstChild().getNodeValue());
  }
  
  /** read optional field boost */
  float readBoost(Element el) {
    NodeList list = el.getElementsByTagName("boost");
    if(list == null || list.getLength() == 0)
      return DomainSuffix.DEFAULT_BOOST;
    return Float.parseFloat(list.item(0).getFirstChild().getNodeValue());
  }
  
  /** read field countryname 
    */
  String readCountryName(Element el) throws IOException {
    NodeList list = el.getElementsByTagName("country");
    if(list == null || list.getLength() == 0)
      throw new IOException("Country name should be given");
    return list.item(0).getNodeValue();
  }
  
  void readSuffixes(DomainSuffixes tldEntries, Element el) {
    NodeList children = el.getElementsByTagName("suffix");
    for(int i=0;i<children.getLength();i++) {
      tldEntries.addDomainSuffix(readSuffix((Element)children.item(i)));
    }
  }

  DomainSuffix readSuffix(Element el) {
    String domain = el.getAttribute("domain");
    Status status = readStatus(el);
    float boost = readBoost(el);
    return new DomainSuffix(domain, status, boost);
  }
  
}
