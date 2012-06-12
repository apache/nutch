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

package org.apache.nutch.parse.headings;

// Nutch imports
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NodeWalker;

// W3C imports
import org.w3c.dom.*;

/**
 * HtmlParseFilter to retrieve h1 and h2 values from the DOM.
 */
public class HeadingsParseFilter implements HtmlParseFilter {

  private Configuration conf;
  private DocumentFragment doc;
  private String[] headings;

  public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {
    this.doc = doc;

    String heading;
    Parse parse = parseResult.get(content.getUrl());

    for (int i = 0 ; headings != null && i < headings.length ; i++ ) {
      heading = getElement(headings[i]);

      if (heading != null) {
        heading.trim();

        if (heading.length() > 0) {
          parse.getData().getParseMeta().set(headings[i], heading);
        }
      }
    }

    return parseResult;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;

    headings = conf.getStrings("headings");
  }

  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Finds the specified element and returns its value
   */
  protected String getElement(String element) {
    NodeWalker walker = new NodeWalker(doc);

    while (walker.hasNext()) {
      Node currentNode = walker.nextNode();

      if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
        if (element.equalsIgnoreCase(currentNode.getNodeName())) {
          return getNodeValue(currentNode);
        }
      }
    }

    // Seems nothing is found
    return null;
  }

  /**
   * Returns the text value of the specified Node and child nodes
   */
  protected static String getNodeValue(Node node) {
    StringBuilder buffer = new StringBuilder();

    NodeList children = node.getChildNodes();

    for (int i = 0; i < children.getLength(); i++) {
      if (children.item(i).getNodeType() == Node.TEXT_NODE) {
        buffer.append(children.item(i).getNodeValue());
      }
    }

    return buffer.toString();
  }

}
