/*
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
import java.io.FileInputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.Date;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.html.DOMBuilder;
import org.apache.nutch.parse.xsl.XslParseFilter.PARSER;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.cyberneko.html.parsers.DOMFragmentParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * A class to group all classic methods to simulate a crawl without running
 * Nutch like setting a configuration, providing a DocumentFragment, etc... All
 * your tests related to parse-xsl shall extend this test.
 * 
 * 
 */
public abstract class AbstractCrawlTest {

  /** The logger used for current and derived classes */
  protected static final Logger LOG = LoggerFactory
      .getLogger(AbstractCrawlTest.class);

  /**
   * the configuration to use with current crawler Never access this property. @see
   * AbstractCrawlTest#getConfiguration()
   */
  private Configuration configuration = null;

  protected String sampleDir = System.getProperty("test.data", ".");

  private long startDate;

  /**
   * @param parseFilter
   *          the filter to use
   * @param filePath
   *          the file to crawl
   * @param url
   *          the url that identifies the file to crawl (only used to set the
   *          unique key)
   * @return the resulting content after the crawl
   * @throws Exception
   */
  protected ParseResult simulateCrawl(PARSER parseFilter, String filePath,
      String url) throws Exception {
    ParseResult result = null;
    FileInputStream is = null;
    try {
      // Opening test file
      File file = new File(filePath);
      is = new FileInputStream(file);
      byte[] bytes = new byte[0];

      // Setting the void content
      Content content = new Content(url, "", bytes, "text/html",
          new Metadata(), this.getConfiguration());

      // Parse document with related parser
      DocumentFragment document = null;
      if (parseFilter == PARSER.NEKO) {
        document = parseNeko(new InputSource(is));

      } else {
        document = parseTagSoup(new InputSource(is));
      }

      // Creates a parser with dedicated method
      HtmlParseFilter filter = new XslParseFilter();
      // Setting configuration
      filter.setConf(this.getConfiguration());

      ParseData data = new ParseData();

      // Initializing the parse result
      ParseResult parseResult = ParseResult.createParseResult(url,
          new ParseImpl("no text", data));

      // Extracting metadata
      result = filter.filter(content, parseResult, null, document);
    } catch (Exception e) {
      throw new Exception("Cannot simulate crawl", e);
    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
          LOG.error("Cannot close input stream", e);
        }
      }
    }
    return result;
  }

  /**
   * Constructs a an html DOM structure.
   * 
   * @param input
   *          the html/xml input stream
   * @return DocumentFragment the document that has been created.
   * @throws Exception
   */
  protected static DocumentFragment parseTagSoup(InputSource input)
      throws Exception {
    HTMLDocumentImpl doc = new HTMLDocumentImpl();
    DocumentFragment frag = doc.createDocumentFragment();
    DOMBuilder builder = new DOMBuilder(doc, frag);
    org.ccil.cowan.tagsoup.Parser reader = new org.ccil.cowan.tagsoup.Parser();
    reader.setContentHandler(builder);
    reader.setFeature(org.ccil.cowan.tagsoup.Parser.ignoreBogonsFeature, true);
    reader.setFeature(org.ccil.cowan.tagsoup.Parser.bogonsEmptyFeature, false);
    reader
        .setProperty("http://xml.org/sax/properties/lexical-handler", builder);
    reader.parse(input);
    return frag;
  }

  /**
   * Constructs a an html DOM structure.
   * 
   * @param input
   *          the html/xml input stream
   * @return DocumentFragment the document that has been created.
   * @throws Exception
   */
  protected static DocumentFragment parseNeko(InputSource input)
      throws Exception {
    DOMFragmentParser parser = new DOMFragmentParser();
    try {
      parser
          .setFeature(
              "http://cyberneko.org/html/features/scanner/allow-selfclosing-iframe",
              true);
      parser.setFeature("http://cyberneko.org/html/features/augmentations",
          true);
      parser.setProperty(
          "http://cyberneko.org/html/properties/default-encoding", "UTF-8");
      parser
          .setFeature(
              "http://cyberneko.org/html/features/scanner/ignore-specified-charset",
              true);
      parser
          .setFeature(
              "http://cyberneko.org/html/features/balance-tags/ignore-outside-content",
              false);
      parser.setFeature(
          "http://cyberneko.org/html/features/balance-tags/document-fragment",
          true);
      parser
          .setFeature("http://cyberneko.org/html/features/balance-tags", true);
      parser.setFeature("http://cyberneko.org/html/features/report-errors",
          true);
      parser.setProperty("http://cyberneko.org/html/properties/names/elems",
          "lower");

      System.out.println(LOG.isTraceEnabled());

    } catch (SAXException e) {
      LOG.error("Cannot set parser features", e);
    }
    // convert Document to DocumentFragment
    HTMLDocumentImpl doc = new HTMLDocumentImpl();
    doc.setErrorChecking(false);
    DocumentFragment res = doc.createDocumentFragment();
    DocumentFragment frag = doc.createDocumentFragment();
    parser.parse(input, frag);
    res.appendChild(frag);

    try {
      while (true) {
        frag = doc.createDocumentFragment();
        parser.parse(input, frag);
        if (!frag.hasChildNodes())
          break;
        // if (LOG.isInfoEnabled()) {
        LOG.info(" - new frag, " + frag.getChildNodes().getLength() + " nodes.");
        System.out.println(" - new frag, " + frag.getChildNodes().getLength()
            + " nodes.");
        // }
        res.appendChild(frag);
      }
    } catch (Exception e) {
      LOG.error("Error: ", e);
      System.out.println(e);
    }

    return res;
  }

  /**
   * 
   * @return the current configuration.
   */
  public Configuration getConfiguration() {
    if (this.configuration == null) {
      this.configuration = NutchConfiguration.create();
    }
    return this.configuration;
  }

  /**
   * To display some memory related information. Can be used for benchmark test
   */
  private void displayMemoryUsage() {
    Runtime runtime = Runtime.getRuntime();

    NumberFormat format = NumberFormat.getInstance();

    long maxMemory = runtime.maxMemory();
    long allocatedMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();

    System.out.println("free memory: " + format.format(freeMemory / 1024));
    System.out.println("allocated memory: "
        + format.format(allocatedMemory / 1024));
    System.out.println("max memory: " + format.format(maxMemory / 1024));
    System.out.println("total free memory: "
        + format.format((freeMemory + (maxMemory - allocatedMemory)) / 1024));
  }

  /**
   * Can be called before each test to get the run test date.
   */
  protected void startTest() {
    System.out.println("Starting test...");
    this.displayMemoryUsage();
    this.startDate = new Date().getTime();
  }

  /**
   * Can be called at the end of a test to evaluate the elapsed time.
   */
  private void endTest() {
    this.displayMemoryUsage();
    System.out.println("Test took " + (new Date().getTime() - this.startDate)
        + " ms");
    System.out.println("Test ended.");
  }

}
