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

import java.io.IOException;
// JDK imports
import java.lang.invoke.MethodHandles;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NodeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import com.optimaize.langdetect.DetectedLanguage;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;

public class HTMLLanguageParser implements HtmlParseFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private int detect = -1, identify = -1;

  private int contentMaxlength = -1;

  private boolean onlyCertain = false;

  private LanguageDetector languageDetector;

  /* A static Map of ISO-639 language codes */
  private static Map<String, String> LANGUAGES_MAP = new HashMap<String, String>();
  static {
    try {
      Properties p = new Properties();
      p.load(HTMLLanguageParser.class
          .getResourceAsStream("langmappings.properties"));
      Enumeration<?> keys = p.keys();
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
   * <ul>
   * <li>1. html lang attribute
   * (http://www.w3.org/TR/REC-html40/struct/dirlang.html#h-8.1) <li>2. meta
   * dc.language
   * (http://dublincore.org/documents/2000/07/16/usageguide/qualified
   * -html.shtml#language) <li>3. meta http-equiv (content-language)
   * (http://www.w3.org/TR/REC-html40/struct/global.html#h-7.4.4.2) <br></ul>
   */
  public ParseResult filter(Content content, ParseResult parseResult,
      HTMLMetaTags metaTags, DocumentFragment doc) {
    String lang = null;

    Parse parse = parseResult.get(content.getUrl());

    if (detect >= 0 && identify < 0) {
      lang = detectLanguage(parse, doc);
    } else if (detect < 0 && identify >= 0) {
      lang = identifyLanguage(parse);
    } else if (detect < identify) {
      lang = detectLanguage(parse, doc);
      if (lang == null) {
        lang = identifyLanguage(parse);
      }
    } else if (identify < detect) {
      lang = identifyLanguage(parse);
      if (lang == null) {
        lang = detectLanguage(parse, doc);
      }
    } else {
      LOG.warn("No configuration for language extraction policy is provided");
      return parseResult;
    }

    if (lang != null) {
      parse.getData().getParseMeta().set(Metadata.LANGUAGE, lang);
      return parseResult;
    }

    return parseResult;
  }

  /** Try to find the document's language from page headers and metadata */
  private String detectLanguage(Parse page, DocumentFragment doc) {
    String lang = getLanguageFromMetadata(page.getData().getParseMeta());
    if (lang == null) {
      LanguageParser parser = new LanguageParser(doc);
      lang = parser.getLanguage();
    }

    if (lang != null) {
      return lang;
    }

    lang = page.getData().getContentMeta().get(Response.CONTENT_LANGUAGE);

    return lang;
  }

  /** Use statistical language identification to extract page language */
  private String identifyLanguage(Parse parse) {
    StringBuilder text = new StringBuilder();
    if (parse == null)
      return null;

    String title = parse.getData().getTitle();
    if (title != null) {
      text.append(title.toString());
    }

    String content = parse.getText();
    if (content != null) {
      text.append(" ").append(content.toString());
    }

    // trim content?
    if (contentMaxlength != -1 && text.length() > contentMaxlength) {
      text.setLength(contentMaxlength);
    }
    List<DetectedLanguage> languages = languageDetector.getProbabilities(text);
    if (languages.isEmpty()) {
    	return null;
    }
    DetectedLanguage lang = languages.get(0);
    if (onlyCertain && lang.getProbability()<0.9) {
      return null;
    }
    return lang.getLocale().getLanguage();

  }

  // Check in the metadata whether the language has already been stored there
  // by Tika
  private static String getLanguageFromMetadata(Metadata meta) {
    if (meta == null)
      return null;
    // dublin core
    String lang = meta.get("dc.language");
    if (lang != null)
      return lang;
    // meta content-language
    lang = meta.get("content-language");
    if (lang != null)
      return lang;
    // lang attribute
    return meta.get("lang");
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
    contentMaxlength = conf.getInt("lang.analyze.max.length", -1);
    onlyCertain = conf.getBoolean("lang.identification.only.certain", false);
    String[] policy = conf.getStrings("lang.extraction.policy");
    for (int i = 0; i < policy.length; i++) {
      if (policy[i].equals("detect")) {
        detect = i;
      } else if (policy[i].equals("identify")) {
        identify = i;
      }
    }
    List<LanguageProfile> languageProfiles;
    try {
      languageProfiles = new LanguageProfileReader().readAllBuiltIn();
      languageDetector = LanguageDetectorBuilder.create(NgramExtractors.standard()).withProfiles(languageProfiles).build();
    } catch (IOException e) {
      LOG.error("Failed loading language profiles", e);
      throw new RuntimeException("Failed loading language profiles");
    }
  }

  public Configuration getConf() {
    return this.conf;
  }

}