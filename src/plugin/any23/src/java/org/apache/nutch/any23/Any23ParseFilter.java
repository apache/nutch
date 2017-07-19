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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.any23.Any23;
import org.apache.any23.extractor.ExtractionParameters;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.writer.BenchmarkTripleHandler;
import org.apache.any23.writer.NTriplesWriter;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.TripleHandlerException;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseFilter;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;

/**
 * <p>This implementation of {@link org.apache.nutch.parse.ParseFilter}
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
public class Any23ParseFilter implements ParseFilter {

  /** Logging instance */
  public static final Logger LOG = LoggerFactory.getLogger(Any23ParseFilter.class);

  private Configuration conf = null;

  /** Constant identifier used as a Key for writing and reading
   * triples to and from the {@link org.apache.nutch.storage.WebPage}
   * metadata Map field.
   */
  public final static String ANY23_TRIPLES = "Any23-Triples";

  private static class Any23Parser {

    Set<String> triples = null;

    Any23Parser(String url, Node node) throws TripleHandlerException {
      triples = new TreeSet<String>();
      try {
        parse(url, node);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e.getReason());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    /**
     * Maintains a {@link java.util.Set} containing the triples 
     * extracted from a {@link org.apache.nutch.storage.WebPage}.</p>
     * @return a {@link java.util.Set} of triples.
     */
    public Set<String> getTriples() {
      return triples;
    }

    private void parse(String url, Node node) throws URISyntaxException, IOException, TripleHandlerException {
      Any23 any23 = new Any23();
      DocumentSource source = any23.createDocumentSource(url);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      TripleHandler tHandler = new NTriplesWriter(baos);
      BenchmarkTripleHandler bHandler = new BenchmarkTripleHandler(tHandler);
      ExtractionParameters eParams = ExtractionParameters.newDefault(); //pass configuration?
      try {
        any23.extract(eParams, source, bHandler);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        tHandler.close();
        bHandler.close();
      }
      //This merely prints out a report of the Any23 extraction.
      LOG.debug("Any23 report: " + bHandler.report()); 
      String n3 = baos.toString("UTF-8");
      // we split the triples stream by the occurrence of 
      // '\n' as this is a distinguishing feature of NTriples
      // output serialization in Any23.
      String[] triplesStrings = n3.split("\n");
      for (String triple: triplesStrings) {
        triples.add(triple);
      }
    }
  }

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.BASE_URL);
    FIELDS.add(WebPage.Field.METADATA);
  }
  /**
   * @see org.apache.nutch.plugin.FieldPluggable#getFields()
   */
  @Override
  public Collection<Field> getFields() {
    return FIELDS;
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
   * @see org.apache.nutch.parse.ParseFilter#filter(java.lang.String, org.apache.nutch.storage.WebPage, org.apache.nutch.parse.Parse, org.apache.nutch.parse.HTMLMetaTags, org.w3c.dom.DocumentFragment)
   */
  @Override
  public Parse filter(String url, WebPage page, Parse parse, 
      HTMLMetaTags metaTags, DocumentFragment doc) {

    Any23Parser parser = null;
    try {
      parser = new Any23Parser(url, doc);
    } catch (TripleHandlerException e) {
      throw new RuntimeException("Error running Any23 parser: " + e.getMessage());
    }
    Set<String> triples = parser.getTriples();
    // can't store multiple values in page metadata -> separate by tabs
    StringBuffer sb = new StringBuffer();
    Iterator<String> iter = triples.iterator();
    while (iter.hasNext()) {
      sb.append(iter.next());
      sb.append("\t");
    }
    ByteBuffer bb = ByteBuffer.wrap(sb.toString().getBytes());
    page.putToMetadata(new Utf8(ANY23_TRIPLES), bb);
    return parse;
  }
}

