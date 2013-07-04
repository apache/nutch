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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NodeWalker;
import org.w3c.dom.*;

/**
 * HtmlParseFilter to retrieve h1 and h2 values from the DOM.
 */
public class HeadingsParseFilter implements HtmlParseFilter {

  /**
   * Pattern used to strip surpluss whitespace
   */
  protected static Pattern whitespacePattern = Pattern.compile("\\s+");
    
  private Configuration conf;
  private DocumentFragment doc;
  private String[] headings;
  private boolean multiValued = false;

  public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {
    this.doc = doc;

    Parse parse = parseResult.get(content.getUrl());

    for (int i = 0 ; headings != null && i < headings.length ; i++ ) {
      List<String> discoveredHeadings = getElement(headings[i]);

      if (discoveredHeadings.size() > 0) {
        for (String heading : discoveredHeadings) {
          if (heading != null) {
            heading.trim();

            if (heading.length() > 0) {
              parse.getData().getParseMeta().add(headings[i], heading);
            }
          }
        }
      }
    }

    return parseResult;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;

    headings = conf.getStrings("headings");
    multiValued = conf.getBoolean("headings.multivalued", false);
  }

  public Configuration getConf() {
    return this.conf;
  }

  /**
   * Finds the specified element and returns its value
   */
  protected List<String> getElement(String element) {
    List<String> headings = new ArrayList<String>();
    NodeWalker walker = new NodeWalker(doc);

    while (walker.hasNext()) {
      Node currentNode = walker.nextNode();

      if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
        if (element.equalsIgnoreCase(currentNode.getNodeName())) {
          headings.add(getNodeValue(currentNode));
          
          // Check for multiValued here, if disabled we don't need
          // to discover more headings.
          if (!multiValued) {
            break;
          }
        }
      }
    }

    return headings;
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

    // Return with stripped surplus whitespace
    Matcher matcher = whitespacePattern.matcher(buffer.toString().trim());
    return matcher.replaceAll(" ").trim();
  }
}
