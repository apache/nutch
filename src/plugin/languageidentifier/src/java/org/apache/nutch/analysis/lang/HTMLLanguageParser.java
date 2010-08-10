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
package org.apache.nutch.analysis.lang;

// JDK imports
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.NodeWalker;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * Adds metadata identifying language of document if found We could also run
 * statistical analysis here but we'd miss all other formats
 */
public class HTMLLanguageParser implements HtmlParseFilter {

  public static final Logger LOG = LoggerFactory.getLogger(HTMLLanguageParser.class);

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  /* A static Map of ISO-639 language codes */
  private static Map LANGUAGES_MAP = new HashMap();
  static {
    try {
      Properties p = new Properties();
      p.load(HTMLLanguageParser.class
          .getResourceAsStream("langmappings.properties"));
      Enumeration keys = p.keys();
      while (keys.hasMoreElements()) {
        String key = (String) keys.nextElement();
        String[] values = p.getProperty(key).split(",", -1);
        LANGUAGES_MAP.put(key, key);
        for (int i = 0; i < values.length; i++) {
          LANGUAGES_MAP.put(values[i].trim().toLowerCase(), key);
        }
      }
    } catch (Exception e) {
      if (LOG.isErrorEnabled()) {
        LOG.error(e.toString());
      }
    }
  }

  private Configuration conf;

  /**
   * Scan the HTML document looking at possible indications of content language<br>
   * <li>1. html lang attribute
   * (http://www.w3.org/TR/REC-html40/struct/dirlang.html#h-8.1) <li>2. meta
   * dc.language
   * (http://dublincore.org/documents/2000/07/16/usageguide/qualified
   * -html.shtml#language) <li>3. meta http-equiv (content-language)
   * (http://www.w3.org/TR/REC-html40/struct/global.html#h-7.4.4.2) <br>
   * Only the first occurence of language is stored.
   */
  public Parse filter(String url, WebPage page, Parse parse,
      HTMLMetaTags metaTags, DocumentFragment doc) {
    String lang = null;
    ByteBuffer blang = getLanguageFromMetadata(page.getMetadata());
    if (blang == null) {
      // Trying to find the document's language
      LanguageParser parser = new LanguageParser(doc);
      lang = parser.getLanguage();
    } else
      lang = Bytes.toString(blang.array());

    if (lang != null) {
      // parse..getParseMeta().set(Metadata.LANGUAGE, lang);
      // TODO where to we store it? in parse or doc?
      page.putToMetadata(new Utf8(Metadata.LANGUAGE), ByteBuffer.wrap(lang
          .getBytes()));
    }
    return parse;
  }

  // Check in the metadata whether the language has already been stored there by
  // Tika
  private static ByteBuffer getLanguageFromMetadata(
      Map<Utf8, ByteBuffer> metadata) {
    if (metadata == null)
      return null;

    // dublin core
    ByteBuffer lang = metadata.get(new Utf8("dc.language"));
    if (lang != null)
      return lang;
    // meta content-language
    lang = metadata.get(new Utf8("content-language"));
    if (lang != null)
      return lang;
    // lang attribute
    return metadata.get(new Utf8("lang"));
  }

  static class LanguageParser {

    private String dublinCore = null;
    private String htmlAttribute = null;
    private String httpEquiv = null;
    private String language = null;

    LanguageParser(Node node) {
      parse(node);
      if (htmlAttribute != null) {
        language = htmlAttribute;
      } else if (dublinCore != null) {
        language = dublinCore;
      } else {
        language = httpEquiv;
      }
    }

    String getLanguage() {
      return language;
    }

    void parse(Node node) {

      NodeWalker walker = new NodeWalker(node);
      while (walker.hasNext()) {

        Node currentNode = walker.nextNode();
        String nodeName = currentNode.getNodeName();
        short nodeType = currentNode.getNodeType();

        String lang = null;

        if (nodeType == Node.ELEMENT_NODE) {

          // Check for the lang HTML attribute
          if (htmlAttribute == null) {
            htmlAttribute = parseLanguage(((Element) currentNode)
                .getAttribute("lang"));
          }

          // Check for Meta
          if ("meta".equalsIgnoreCase(nodeName)) {
            NamedNodeMap attrs = currentNode.getAttributes();

            // Check for the dc.language Meta
            if (dublinCore == null) {
              for (int i = 0; i < attrs.getLength(); i++) {
                Node attrnode = attrs.item(i);
                if ("name".equalsIgnoreCase(attrnode.getNodeName())) {
                  if ("dc.language".equalsIgnoreCase(attrnode.getNodeValue())) {
                    Node valueattr = attrs.getNamedItem("content");
                    if (valueattr != null) {
                      dublinCore = parseLanguage(valueattr.getNodeValue());
                    }
                  }
                }
              }
            }

            // Check for the http-equiv content-language
            if (httpEquiv == null) {
              for (int i = 0; i < attrs.getLength(); i++) {
                Node attrnode = attrs.item(i);
                if ("http-equiv".equalsIgnoreCase(attrnode.getNodeName())) {
                  if ("content-language".equals(attrnode.getNodeValue()
                      .toLowerCase())) {
                    Node valueattr = attrs.getNamedItem("content");
                    if (valueattr != null) {
                      httpEquiv = parseLanguage(valueattr.getNodeValue());
                    }
                  }
                }
              }
            }
          }
        }

        if ((dublinCore != null) && (htmlAttribute != null)
            && (httpEquiv != null)) {
          return;
        }
      }
    }

    /**
     * Parse a language string and return an ISO 639 primary code, or
     * <code>null</code> if something wrong occurs, or if no language is found.
     */
    final static String parseLanguage(String lang) {

      if (lang == null) {
        return null;
      }

      String code = null;
      String language = null;

      // First, split multi-valued values
      String langs[] = lang.split(",| |;|\\.|\\(|\\)|=", -1);

      int i = 0;
      while ((language == null) && (i < langs.length)) {
        // Then, get the primary code
        code = langs[i].split("-")[0];
        code = code.split("_")[0];
        // Find the ISO 639 code
        language = (String) LANGUAGES_MAP.get(code.toLowerCase());
        i++;
      }

      return language;
    }

  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

  public Collection<Field> getFields() {
    return FIELDS;
  }
}
