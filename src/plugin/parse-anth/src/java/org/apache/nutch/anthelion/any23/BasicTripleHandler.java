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
package org.apache.nutch.anthelion.any23;

import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.any23.extractor.ExtractionContext;
import org.apache.any23.vocab.SINDICE;
import org.apache.any23.vocab.XHTML;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.TripleHandlerException;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

/**
 * THIS IS NOT USED Excludes not real triples (like titles, css and other
 * irrelevant metatags)
 * 
 * @author Petar Ristoski (petar@dwslab.de)
 *
 */
public class BasicTripleHandler implements TripleHandler {

  public final static List<String> EXTRACTORS = Arrays.asList("html-rdfa", "html-microdata", "html-mf-geo",
      "html-mf-hcalendar", "html-mf-hcard", "html-mf-hlisting", "html-mf-hresume", "html-mf-hreview",
      "html-mf-species", "html-mf-hrecipe", "html-mf-xfn", "html-head-meta");

  // exclude namesspaces which cause title and css links to be included as
  // triples
  public final static List<String> evilNamespaces = Arrays.asList(XHTML.NS, SINDICE.NS);
  // allow namespaces which are subsets of evilNamespaces to be included
  // TODO check if other namesspaces must be included as well here
  public final static List<String> notSoEvilNamespaces = Arrays.asList("http://vocab.sindice.net/any23#hrecipe/");

  private TripleHandler wrapped = null;

  private long totalTriples = 0;
  private AtomicInteger totalDocuments = new AtomicInteger(0);
  private final Collection<String> extractorNames = new HashSet<String>();

  private Map<String, Long> triplesPerExtractor = new HashMap<String, Long>();
  private List<String> negativeFilterNamespaces;
  private List<String> positiveFilterNamespaces;
  private OutputStreamWriter writer;
  private boolean started = false;
  private Map<String, String> namespaceTable;

  public BasicTripleHandler(TripleHandler wrapped, List<String> negativeFilterNamespaces,
      List<String> positivFilterNamespaces) {

    this.negativeFilterNamespaces = negativeFilterNamespaces;
    this.positiveFilterNamespaces = positivFilterNamespaces;
    for (String ex : EXTRACTORS) {
      triplesPerExtractor.put(ex, new Long(0));
    }
    if (wrapped == null) {
      throw new NullPointerException("wrapped cannot be null.");
    }
    this.wrapped = wrapped;

  }

  public Collection<String> getExtractorNames() {
    return extractorNames;
  }

  public long getTotalTriplesFound() {
    return totalTriples;
  }

  public Map<String, Long> getTriplesPerExtractor() {
    return triplesPerExtractor;
  }

  public int getTotalDocuments() {
    return totalDocuments.get();
  }

  /**
   * @return a human readable report.
   */
  public String printReport() {
    return String.format("Total Documents: %d, Total Triples: %d", getTotalDocuments(), getTotalTriplesFound());
  }

  public void startDocument(URI documentURI) throws TripleHandlerException {
    totalDocuments.incrementAndGet();
    wrapped.startDocument(documentURI);
  }

  public void openContext(ExtractionContext context) throws TripleHandlerException {
    wrapped.openContext(context);
  }

  public void receiveNamespace(String prefix, String uri, ExtractionContext context) throws TripleHandlerException {
    wrapped.receiveNamespace(prefix, uri, context);
  }

  public void receiveTriple(Resource s, URI p, Value o, URI g, ExtractionContext context)
      throws TripleHandlerException {
    // if uri is in negative namespace which has to be filtered out and not
    // in the positive list - return directly
    for (String negativeFilterNamespace : negativeFilterNamespaces) {
      if (p.toString().startsWith(negativeFilterNamespace)) {
        for (String positiveFilterNamespace : positiveFilterNamespaces) {
          if (!p.toString().startsWith(positiveFilterNamespace)) {
            // log.debug("Namespace filtered: "
            // + s.toString() + " , " + p.toString() + ", "
            // + o.toString());
            return;
          }
        }
      }
    }
    extractorNames.add(context.getExtractorName());
    totalTriples++;
    wrapped.receiveTriple(s, p, o, g, context);

  }

  public void setContentLength(long contentLength) {
    wrapped.setContentLength(contentLength);
  }

  public void closeContext(ExtractionContext context) throws TripleHandlerException {
    wrapped.closeContext(context);
  }

  public void endDocument(URI documentURI) throws TripleHandlerException {
    wrapped.endDocument(documentURI);
  }

  public void close() throws TripleHandlerException {
    wrapped.close();
  }

}
