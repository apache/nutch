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
package org.apache.nutch.anthelion.parsing;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.any23.extractor.ExtractionContext;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.TripleHandlerException;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.ntriples.NTriplesUtil;

/**
 * Excludes not real triples (like titles, css and other irrelevant metatags)
 * 
 * @author LOD2 group by Sebastian Hellmann
 * @author Petar Ristoski (petar@dwslab.de)
 *
 */
public class FilterableTripleHandler implements TripleHandler {

  private long totalTriples = 0;
  private Map<String, Long> triplesPerExtractor = new HashMap<String, Long>();
  private List<String> negativeFilterNamespaces;
  private List<String> positiveFilterNamespaces;
  private StringBuffer writer;
  private boolean started = false;
  private Map<String, String> namespaceTable;
  public final static List<String> EXTRACTORS = Arrays.asList("html-rdfa", "html-rdfa11", "html-microdata",
      "html-mf-geo", "html-mf-hcalendar", "html-mf-hcard", "html-mf-hlisting", "html-mf-hresume",
      "html-mf-hreview", "html-mf-species", "html-mf-hrecipe", "html-mf-xfn", "html-head-meta");

  // it should already remove css and other stuff but it doesn't
  public final static List<String> eveilNamespacesCSS = Arrays.asList("stylesheet", "css", "icon", "shortcut");

  /**
   * Create a FilterableTripleHandler which allows the controlled exclusion of
   * triples belonging to namespaces which are not wanted
   * 
   * @param osw
   *            The {@link OutputStreamWriter}
   * @param negativeFilterNamespaces
   *            List of namespaces which should be left out
   * @param positivFilterNamespaces
   *            List of namesspaces within the negative namesspace list which
   *            shouldbe still included
   */
  public FilterableTripleHandler(StringBuffer osw, List<String> negativeFilterNamespaces,
      List<String> positivFilterNamespaces) {
    this.writer = osw;
    this.negativeFilterNamespaces = negativeFilterNamespaces;
    this.positiveFilterNamespaces = positivFilterNamespaces;
    for (String ex : EXTRACTORS) {
      triplesPerExtractor.put(ex, new Long(0));
    }
  }

  Map<String, URI> extractorNames = new HashMap<String, URI>();

  public void receiveTriple(Resource s, URI p, Value o, URI g, ExtractionContext context)
      throws TripleHandlerException {
    // if uri is in negative namespace which has to be filtered out and not
    // in the positive list - return directly
    for (String negativeFilterNamespace : negativeFilterNamespaces) {
      if (p.toString().startsWith(negativeFilterNamespace)) {
        for (String positiveFilterNamespace : positiveFilterNamespaces) {
          if (!p.toString().startsWith(positiveFilterNamespace)) {
            System.out.println(
                "Namespace filtered: " + s.toString() + " , " + p.toString() + ", " + o.toString());
            return;
          }
        }
      }
    }

    // check the css
    for (String negativeFileter : eveilNamespacesCSS)
      if (p.toString().toLowerCase().contains(negativeFileter))
        return;

    URI extractorUri = extractorNames.get(context.getExtractorName());
    if (extractorUri == null) {
      extractorUri = ValueFactoryImpl.getInstance().createURI("ex:" + context.getExtractorName());
      extractorNames.put(context.getExtractorName(), extractorUri);
    }

    try {
      handleStatement(s, p, o, context.getDocumentURI(), extractorUri);
      totalTriples++;
    } catch (RDFHandlerException e) {
      throw new TripleHandlerException("Unable to recieve Triple", e);
    }

    String ex = context.getExtractorName();
    if (triplesPerExtractor.containsKey(ex)) {
      triplesPerExtractor.put(ex, new Long(triplesPerExtractor.get(ex) + 1));
    }
  }

  public long getTotalTriplesFound() {
    return totalTriples;
  }

  public Map<String, Long> getTriplesPerExtractor() {
    return triplesPerExtractor;
  }

  @Override
  public void startDocument(URI documentURI) throws TripleHandlerException {
    started = true;
  }

  @Override
  public void openContext(ExtractionContext context) throws TripleHandlerException {
    // ignore
  }

  @Override
  public void receiveNamespace(String prefix, String uri, ExtractionContext context) throws TripleHandlerException {
    if (!started) {
      throw new IllegalStateException("Parsing never started.");
    }

    if (namespaceTable == null) {
      namespaceTable = new HashMap<String, String>();
    }
    namespaceTable.put(prefix, NTriplesUtil.escapeString(uri));

  }

  public void handleStatement(Resource subject, URI predicate, Value object, Resource... contexts)
      throws RDFHandlerException {
    if (!started) {
      throw new IllegalStateException("Cannot handle statement without start parsing first.");
    }

    try {
      printResource(subject);
      printSpace();
      printURI(predicate);
      printSpace();
      printObject(object);
      printSpace();

      for (int i = 0; i < contexts.length; i++) {
        printResource(contexts[i]);
        printSpace();
      }

      printCloseStatement();
    } catch (IOException ioe) {
      throw new RDFHandlerException("An error occurred while printing statement.", ioe);
    }
  }

  @Override
  public void closeContext(ExtractionContext context) throws TripleHandlerException {
    // ignore
  }

  @Override
  public void endDocument(URI documentURI) throws TripleHandlerException {
    // ignore
  }

  @Override
  public void setContentLength(long contentLength) {
    // ignore

  }

  @Override
  public void close() throws TripleHandlerException {
    if (!started) {
      throw new IllegalStateException("Parsing never started.");
    }
    started = false;
    if (namespaceTable != null) {
      namespaceTable.clear();
    }
  }

  private void printSpace() throws IOException {
    writer.append(' ');
  }

  private void printCloseStatement() throws IOException {
    writer.append(" .\n");
  }

  private void printURI(URI uri) throws IOException {
    final String uriString = uri.stringValue();
    int splitIdx = 0;
    String namespace = null;
    if (namespaceTable != null) {
      splitIdx = uriString.indexOf(':');
      if (splitIdx > 0) {
        String prefix = uriString.substring(0, splitIdx);
        namespace = namespaceTable.get(prefix);
      }
    }

    if (namespace != null) {
      writer.append('<');
      writer.append(namespace);
      writer.append(NTriplesUtil.escapeString(uriString.substring(splitIdx)));
      writer.append('>');
    } else {
      writer.append('<');
      writer.append(NTriplesUtil.escapeString(uriString));
      writer.append('>');
    }
  }

  private void printBNode(BNode b) throws IOException {
    writer.append(NTriplesUtil.toNTriplesString(b));
  }

  private void printResource(Resource r) throws IOException {
    if (r instanceof BNode) {
      printBNode((BNode) r);
    } else if (r instanceof URI) {
      printURI((URI) r);
    } else {
      throw new IllegalStateException();
    }
  }

  private void printLiteral(Literal l) throws IOException {
    writer.append(NTriplesUtil.toNTriplesString(l));
  }

  private void printObject(Value v) throws IOException {
    if (v instanceof Resource) {
      printResource((Resource) v);
      return;
    }
    printLiteral((Literal) v);
  }
}
