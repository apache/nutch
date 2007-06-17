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
package org.apache.nutch.microformats.reltag;

// JDK imports
import java.net.URL;
import java.net.URLDecoder;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

// Nutch imports
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.StringUtil;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;


/**
 * Adds microformat rel-tags of document if found.
 *
 * @see <a href="http://www.microformats.org/wiki/rel-tag">
 *      http://www.microformats.org/wiki/rel-tag</a>
 * @author J&eacute;r&ocirc;me Charron
 */
public class RelTagParser implements HtmlParseFilter {
  
  public final static Log LOG = LogFactory.getLog(RelTagParser.class);

  public final static String REL_TAG = "Rel-Tag";
  
  
  private Configuration conf = null;
  
  
  /**
   * Scan the HTML document looking at possible rel-tags
   */
  public ParseResult filter(Content content, ParseResult parseResult,
    HTMLMetaTags metaTags, DocumentFragment doc) {
    
    // get parse obj
    Parse parse = parseResult.get(content.getUrl());
    // Trying to find the document's rel-tags
    Parser parser = new Parser(doc);
    Set tags = parser.getRelTags();
    Iterator iter = tags.iterator();
    Metadata metadata = parse.getData().getParseMeta();
    while (iter.hasNext()) {
      metadata.add(REL_TAG, (String) iter.next());
    }
    return parseResult;
  }

  private static class Parser {

    Set tags = null;
    
    Parser(Node node) {
      tags = new TreeSet();
      parse(node);
    }
  
    Set getRelTags() {
      return tags;
    }
    
    void parse(Node node) {

      if (node.getNodeType() == Node.ELEMENT_NODE) {
        // Look for <a> tag
        if ("a".equalsIgnoreCase(node.getNodeName())) {
          NamedNodeMap attrs = node.getAttributes();
          Node hrefNode = attrs.getNamedItem("href");
          // Checks that it contains a href attribute
          if (hrefNode != null) {
            Node relNode = attrs.getNamedItem("rel");
            // Checks that it contains a rel attribute too
            if (relNode != null) {
              // Finaly checks that rel=tag
              if ("tag".equalsIgnoreCase(relNode.getNodeValue())) {
                String tag = parseTag(hrefNode.getNodeValue());
                if (!StringUtil.isEmpty(tag)) {
                  tags.add(tag);
                }
              }
            }
          }
        }
      }
      
      // Recurse
      NodeList children = node.getChildNodes();
      for (int i=0; children != null && i<children.getLength(); i++) {
        parse(children.item(i));
      }
    }
    
    private final static String parseTag(String url) {
      String tag = null;
      try {
        URL u = new URL(url);
        String path = u.getPath();
        tag = URLDecoder.decode(path.substring(path.lastIndexOf('/') + 1), "UTF-8");
      } catch (Exception e) {
        // Malformed tag...
        tag = null;
      }
      return tag;
    }
    
  }


  /* ----------------------------- *
   * <implementation:Configurable> *
   * ----------------------------- */
  
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }
  
  /* ------------------------------ *
   * </implementation:Configurable> *
   * ------------------------------ */
  
}
