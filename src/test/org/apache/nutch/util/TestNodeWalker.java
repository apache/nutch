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

package org.apache.nutch.util;

import java.io.ByteArrayInputStream;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.xerces.parsers.DOMParser;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

/** Unit tests for NodeWalker methods. */
public class TestNodeWalker {

  /* a snapshot of the nutch webpage */
  private final static String WEBPAGE= 
  "<html xmlns=\"http://www.w3.org/1999/xhtml\" lang=\"en\" xml:lang=\"en\"><head><title>Nutch</title></head>"
  + "<body>"
  + "<ul>"
  + "<li>crawl several billion pages per month</li>"
  + "<li>maintain an index of these pages</li>"
  + "<li>search that index up to 1000 times per second</li>"
  + "<li>provide very high quality search results</li>"
  + "<li>operate at minimal cost</li>"
  + "</ul>"
  + "</body>"
  + "</html>";

  private final static String[] ULCONTENT = new String[4];
  
  @Before
  public void setUp() throws Exception{
    ULCONTENT[0]="crawl several billion pages per month" ;
    ULCONTENT[1]="maintain an index of these pages" ;
    ULCONTENT[2]="search that index up to 1000 times per second"  ;
    ULCONTENT[3]="operate at minimal cost" ;
  }

  @Test
  public void testSkipChildren() {
    DOMParser parser= new DOMParser();
    
    try {
      parser.setFeature("http://xml.org/sax/features/validation", false);
      parser.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
      parser.parse(new InputSource(new ByteArrayInputStream(WEBPAGE.getBytes())));
    } catch (Exception e) {
      e.printStackTrace();
    }
     
    StringBuffer sb = new StringBuffer();
    NodeWalker walker = new NodeWalker(parser.getDocument());
    while (walker.hasNext()) {
      Node currentNode = walker.nextNode();
      short nodeType = currentNode.getNodeType();
      if (nodeType == Node.TEXT_NODE) {
        String text = currentNode.getNodeValue();
        text = text.replaceAll("\\s+", " ");
        sb.append(text);
      }
    }
   assertTrue("UL Content can NOT be found in the node", findSomeUlContent(sb.toString()));
     
   StringBuffer sbSkip = new StringBuffer();
   NodeWalker walkerSkip = new NodeWalker(parser.getDocument());
   while (walkerSkip.hasNext()) {
     Node currentNode = walkerSkip.nextNode();
     String nodeName = currentNode.getNodeName();
     short nodeType = currentNode.getNodeType();
     if ("ul".equalsIgnoreCase(nodeName)) {
       walkerSkip.skipChildren();
     }
     if (nodeType == Node.TEXT_NODE) {
       String text = currentNode.getNodeValue();
       text = text.replaceAll("\\s+", " ");
       sbSkip.append(text);
     }
   }
   assertFalse("UL Content can be found in the node", findSomeUlContent(sbSkip.toString()));
  }
  
  public boolean findSomeUlContent(String str) {
    for(int i=0; i<ULCONTENT.length ; i++){
      if(str.contains(ULCONTENT[i])) return true;
    }    
    return false;
  }
}
