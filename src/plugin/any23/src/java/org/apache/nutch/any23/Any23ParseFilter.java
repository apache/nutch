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
package org.apache.nutch.any23;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.*;

import org.apache.any23.Any23;
import org.apache.any23.writer.BenchmarkTripleHandler;
import org.apache.any23.writer.NTriplesWriter;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.TripleHandlerException;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.*;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

/**
 * <p>This implementation of {@link org.apache.nutch.parse.HtmlParseFilter}
 * uses the <a href="http://any23.apache.org">Apache Any23</a> library
 * for parsing and extracting structured data in RDF format from a
 * variety of Web documents. Currently it supports the following
 * input formats:</p>
 * <ol><li>RDF/XML, Turtle, Notation 3</li>
 * <li>RDFa with RDFa1.1 prefix mechanism</li>
 * <li>Microformats: Adr, Geo, hCalendar, hCard, hListing, hResume, hReview,
 * License, XFN and Species</li>
 * <li>HTML5 Microdata: (such as Schema.org)</li>
 * <li>CSV: Comma Separated Values with separator autodetection.</li></ol>.
 * <p>In this implementation triples are written as Notation3 e.g.
 * <code><http://www.bbc.co.uk/news/scotland/> <http://iptc.org/std/rNews/2011-10-07#datePublished> "2014/03/31 13:53:03"@en-gb .</code>
 * and triples are identified within output triple streams by the presence of '\n'.
 * The presence of the '\n' is a characteristic specific to N3 serialization in Any23.
 * In order to use another/other writers implementing the
 * <a href="http://any23.apache.org/apidocs/index.html?org/apache/any23/writer/TripleHandler.html">TripleHandler</a>
 * interface, we will most likely need to identify an alternative data characteristic
 * which we can use to split triples streams.</p>
 * <p>
 *
 */
public class Any23ParseFilter implements HtmlParseFilter {

  /** Logging instance */
  public static final Logger LOG = LoggerFactory.getLogger(Any23ParseFilter.class);

  private Configuration conf = null;

  /** Constant identifier used as a Key for writing and reading
   * triples to and from the metadata Map field.
   */
  public final static String ANY23_TRIPLES = "Any23-Triples";

  private static class Any23Parser {

    Set<String> triples = null;

    Any23Parser(String url, String htmlContent) throws TripleHandlerException {
      triples = new TreeSet<String>();
      try {
        parse(url, htmlContent);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e.getReason());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    /**
     * Maintains a {@link java.util.Set} containing the triples
     * @return a {@link java.util.Set} of triples.
     */
    public Set<String> getTriples() {
      return triples;
    }

    private void parse(String url, String htmlContent) throws URISyntaxException, IOException, TripleHandlerException {
      Any23 any23 = new Any23();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      TripleHandler tHandler = new NTriplesWriter(baos);
      BenchmarkTripleHandler bHandler = new BenchmarkTripleHandler(tHandler);
      try {
        any23.extract(htmlContent, url, "text/HTML", "UTF-8", bHandler);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        tHandler.close();
        bHandler.close();
      }
      //This merely prints out a report of the Any23 extraction.
      LOG.info("Any23 report: " + bHandler.report());
      String n3 = baos.toString("UTF-8");
      // we split the triples stream by the occurrence of
      // '\n' as this is a distinguishing feature of NTriples
      // output serialization in Any23.
      String[] triplesStrings = n3.split("\n");
      Collections.addAll(triples, triplesStrings);
    }
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#getConf()
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * @see org.apache.nutch.parse.HtmlParseFilter#filter(Content, ParseResult, HTMLMetaTags, DocumentFragment)
   */
  @Override
  public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {

    Any23Parser parser = null;
    try {
      String htmlContent = new String(content.getContent(), Charset.forName("UTF-8"));
      parser = new Any23Parser(content.getUrl(), htmlContent);
    } catch (TripleHandlerException e) {
      throw new RuntimeException("Error running Any23 parser: " + e.getMessage());
    }
    Set<String> triples = parser.getTriples();
    // can't store multiple values in page metadata -> separate by tabs
    StringBuilder sb = new StringBuilder();

    Parse parse = parseResult.get(content.getUrl());
    Metadata metadata = parse.getData().getParseMeta();

    for (String triple : triples) {
      sb.append(triple);
      sb.append("\t");
    }
    metadata.add(ANY23_TRIPLES, sb.toString());

    return parseResult;
  }
}

