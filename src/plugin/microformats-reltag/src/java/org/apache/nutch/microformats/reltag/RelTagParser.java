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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.ParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Adds microformat rel-tags of document if found.
 * 
 * @see <a href="http://www.microformats.org/wiki/rel-tag">
 *      http://www.microformats.org/wiki/rel-tag</a>
 * @author J&eacute;r&ocirc;me Charron
 */
public class RelTagParser implements ParseFilter {

  public static final Logger LOG = LoggerFactory.getLogger(RelTagParser.class);

  public final static String REL_TAG = "Rel-Tag";

  private Configuration conf = null;

  private static class Parser {

    Set<String> tags = null;

    Parser(Node node) {
      tags = new TreeSet<String>();
      parse(node);
    }

    Set<String> getRelTags() {
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
	          if(!tags.contains(tag)){
                    tags.add(tag);
		    LOG.debug("Adding tag: " + tag + " to tag set.");
                  }
	        }
	      }
	    }
	  }
	}
      }

      // Recurse
      NodeList children = node.getChildNodes();
      for (int i = 0; children != null && i < children.getLength(); i++) {
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
      } return tag;
    }
  }

  /**
   * Set the {@link Configuration} object
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get the {@link Configuration} object
   */
  public Configuration getConf() {
    return this.conf;
  }

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.BASE_URL);
    FIELDS.add(WebPage.Field.METADATA);
  }
  
  /**
   * Gets all the fields for a given {@link WebPage}
   * Many datastores need to setup the mapreduce job by specifying the fields
   * needed. All extensions that work on WebPage are able to specify what fields
   * they need.
   */
  @Override
  public Collection<Field> getFields() {
    return FIELDS;
  }

  @Override
  /**
   * Scan the HTML document looking at possible rel-tags
   * @param url URL of the {@link WebPage} to be parsed 
   * @param page {@link WebPage} object relative to the URL
   * @param parse {@link Parse} object holding parse status
   * @param metatags within the {@link NutchDocument}
   * @param doc The {@link NutchDocument} object
   * @return parse the actual {@link Parse} object
   */
  public Parse filter(String url, WebPage page, Parse parse,
      HTMLMetaTags metaTags, DocumentFragment doc) {
    // Trying to find the document's rel-tags
    Parser parser = new Parser(doc);
    Set<String> tags = parser.getRelTags();
    // can't store multiple values in page metadata -> separate by tabs
    StringBuffer sb = new StringBuffer();
    Iterator<String> iter = tags.iterator();
    while (iter.hasNext()) {
      sb.append(iter.next());
      sb.append("\t");
    }
    ByteBuffer bb = ByteBuffer.wrap(sb.toString().getBytes());
    page.putToMetadata(new Utf8(REL_TAG), bb);
    return parse;
  }
}
