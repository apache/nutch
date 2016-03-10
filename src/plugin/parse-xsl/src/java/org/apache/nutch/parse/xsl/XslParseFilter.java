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

package org.apache.nutch.parse.xsl;

import java.io.File;
import java.io.FileOutputStream;

import javax.xml.bind.JAXB;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.xsl.xml.document.Documents;
import org.apache.nutch.parse.xsl.xml.document.TDocument;
import org.apache.nutch.parse.xsl.xml.document.TField;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;

import com.sun.org.apache.xpath.internal.XPathAPI;

/**
 * This is a parse filter plugin (@see HtmlParseFilter) A class to apply an xsl
 * transformation on an html page. Instead of coding java, a simple xpath can be
 * used.
 * 
 */
public class XslParseFilter implements HtmlParseFilter {

  /** Specifies whether to use html parse TagSoup or NekoHtml */
  public enum PARSER {
    /** TagSoup parser */
    TAGSOUP {
      @Override
      public String toString() {
        return "tagsoup";
      }
    },
    /** Neko parser */
    NEKO {
      @Override
      public String toString() {
        return "neko";
      }
    }
  }

  /**
   * The output of the transformation for debug purpose (log level "DEBUG" shall
   * be activated)
   */
  public static final String CONF_XSLT_OUTPUT_DEBUG_FILE = "parser.xsl.output.debug.file";

  /** Whether to use Saxon or Standard JVM XSLT parser */
  public static final String CONF_XSLT_USE_SAXON = "parser.xsl.useSaxon";

  /**
   * Whether to use Neko or Tagsoup.
   * 
   * @Warning this configuration property is set by Nutch and not by the current
   *          plugin. see HtmlParser
   */
  public static final String CONF_HTML_PARSER = "parser.html.impl";

  private static final Logger LOG = LoggerFactory
      .getLogger(XslParseFilter.class);

  private Configuration conf;

  // The html parser to use (default is neko. Otherwise Tag Soup)
  private String parser;
  // The xsl parser to use (default from jvm or Saxon)
  private boolean ifSaxonParser;
  // Debug file to use
  private String debugFile;

  // The XXX
  private RulesManager manager;

  /**
   * Default constructor forbidden.
   */
  public XslParseFilter() {
    super();
  }

  /**
   * @param content
   *          full content to parse
   * @param parseResult
   *          result of the parse process
   * @param metaTags
   *          metatags set in the document
   * @param document
   *          the DOM document to parse
   * @return the resulting {@link ParseResult}
   */
  @Override
  public ParseResult filter(Content content, ParseResult parseResult,
      HTMLMetaTags metaTags, DocumentFragment document) {

    if (manager == null) {
      // no RulesManager, nothing to do
      return parseResult;
    }

    Transformer transformer = manager.getTransformer(content.getUrl());
    if (transformer == null) {
      return parseResult;
    }

    try {
      // We are selecting the HTML tag with a XPath to convert the
      // DocumentFragment to a more natural
      // HTML document that can be further processed with XSL.
      // TODO applying an "html" xpath is a dirty trick to change.
      String xpath = "html";

      // For neko, all tags are UPPER CASE.
      // For tagsoup, it is in lower case.
      // This is decided by the html parser plugin
      if (this.parser.equals(PARSER.NEKO.toString())) {
        xpath = xpath.toUpperCase();
      } else {
        // TODO Tag soup is not working. To be investigated.
        throw new Exception("tag soup parser not implemented.");
      }

      Node doc = XPathAPI.selectSingleNode(document, xpath);

      Parse parse = parseResult.get(content.getUrl());

      DOMResult result = new DOMResult();
      // At this state, thanks to the HtmlParser that is using
      // HtmlParseFilter interface, we got
      // a DOM object properly built (with Neko or TagSoup).
      transformer.transform(new DOMSource(doc), result);

      // Storing the xml output for debug purpose
      if (LOG.isDebugEnabled() && this.debugFile != null) {
        XslParseFilter.saveDOMOutput(doc, new File(debugFile));
        // XslParseFilter.saveDOMOutput(result.getNode(), new File(debugFile));
      }

      XslParseFilter.updateMetadata(result.getNode(), parse);

    } catch (Exception e) {
      LOG.warn("Cannot extract HTML tags. The XSL processing will not be run.",
          e);
    }

    return parseResult;
  }

  /**
   * @param node
   *          the node that is used to provide metadata information.
   * @param data
   *          the data to update This is a simple format like the following:
   *          Check the documents.xsd to figure out the structure.
   */
  protected static void updateMetadata(Node node, Parse data) {

    Documents documents = JAXB.unmarshal(new DOMSource(node), Documents.class);

    // No document unmarshalled
    if (documents == null) {
      LOG.debug("No metadata to update");
      return;
    }

    // Browsing documents
    for (TDocument document : documents.getDocument()) {

      // There are metadata to process
      for (TField field : document.getField()) {
        String value = field.getValue();
        // Trim values by default
        if (value != null) {
          value = value.trim();
          // Do not keep string with 0 size
          if (value.length() != 0) {
            // Adds the meta to the parse meta list
            data.getData().getParseMeta().add(field.getName(), value);
          }
          if (LOG.isDebugEnabled())
            LOG.debug("Content " + field.getName() + " has value: '" + value
                + "'");
        }
      }
    }

  }

  /**
   * 
   * @param node
   *          the DOM node to save.
   * @param file
   *          the file where to write the DOM.
   */
  private static void saveDOMOutput(Node node, File file) {
    FileOutputStream fos = null;

    try {
      fos = new FileOutputStream(file);

      TransformerFactory.newInstance().newTransformer()
          .transform(new DOMSource(node), new StreamResult(fos));
    } catch (Exception e) {
      LOG.warn("Cannot store DOM node to file: " + file.getAbsolutePath(), e);
    } finally {
      if (fos != null)
        try {
          fos.close();
        } catch (Exception e) {
          LOG.warn("Cannot close xml file stream.", e);
        }
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    // Setting the parser from conf
    parser = this.conf.get(CONF_HTML_PARSER, PARSER.NEKO.toString());
    // Setting the parser to use from conf
    ifSaxonParser = this.conf.getBoolean(CONF_XSLT_USE_SAXON, false);
    // Debug file to use
    debugFile = this.conf.get(CONF_XSLT_OUTPUT_DEBUG_FILE);

    // TODO: use saxon for xslt 2.0 compliancy
    if (this.ifSaxonParser) {
      System.setProperty("javax.xml.transform.TransformerFactory",
          "net.sf.saxon.TransformerFactoryImpl");
    }

    // create rules manager and load all configuration files
    manager = new RulesManager(conf);
  }

}
