/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.clustering.carrot2;

import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

import org.apache.xerces.parsers.AbstractSAXParser;
import org.cyberneko.html.HTMLConfiguration;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import org.apache.nutch.searcher.HitDetails;

import com.dawidweiss.carrot.core.local.LocalInputComponentBase;
import com.dawidweiss.carrot.core.local.ProcessingException;
import com.dawidweiss.carrot.core.local.RequestContext;
import com.dawidweiss.carrot.core.local.clustering.*;

/**
 * A local input component that ignores the query passed from the
 * controller and instead looks for data stored in the request context.
 * This enables us to reuse the same physical component implementation
 * for data that has already been acquired from Nutch.    
 *
 * @author Dawid Weiss
 * @version $Id: LocalNutchInputComponent.java,v 1.1 2004/08/09 23:23:53 johnnx Exp $
 */
public class LocalNutchInputComponent extends LocalInputComponentBase {
  public final static String NUTCH_INPUT_HIT_DETAILS_ARRAY
    = "NUTCH_INPUT_HIT_DETAILS_ARRAY";

  public final static String NUTCH_INPUT_SUMMARIES_ARRAY 
    = "NUTCH_INPUT_SUMMARIES_ARRAY";

  /** Capabilities required from the next component in the chain */
  private final static Set SUCCESSOR_CAPABILITIES 
    = new HashSet(Arrays.asList(new Object [] { RawDocumentsConsumer.class }));

  /** This component's capabilities */
  private final static Set COMPONENT_CAPABILITIES 
    = new HashSet(Arrays.asList(new Object [] { RawDocumentsProducer.class }));
    
  /*
   * @see com.dawidweiss.carrot.core.local.LocalInputComponent#setQuery(java.lang.String)
   */
  public void setQuery(String query) {
      // ignore the query; data will be provided from the request context.
  }

  /** A callback hook that starts the processing. */
  public void startProcessing(RequestContext context) throws ProcessingException {
    // let successor components know that the processing has started.
    super.startProcessing(context);
    
    // get the information about documents from the context.
    Map params = context.getRequestParameters();
    HitDetails [] details = (HitDetails[]) params.get(NUTCH_INPUT_HIT_DETAILS_ARRAY);
    String [] summaries = (String[]) params.get(NUTCH_INPUT_SUMMARIES_ARRAY);
    
    if (details == null)
      throw new ProcessingException("Details array must not be null.");

    if (summaries == null)
      throw new ProcessingException("Summaries array must not be null.");
    
    if (summaries.length != details.length)
      throw new ProcessingException("Summaries and details must be of the same length.");
    
    RawDocumentsConsumer consumer = (RawDocumentsConsumer) next;
    
    // produce 'documents' for successor components.
    for (int i=0;i<summaries.length;i++) {
      consumer.addDocument(new NutchDocument(i, details[i], htmlToText(summaries[i])));
    }
  }

  /**
   * Returns the capabilities provided by this component.
   */
  public Set getComponentCapabilities() {
    return COMPONENT_CAPABILITIES;
  }
    
  /**
   * Returns the capabilities required from the successor component.
   */
  public Set getRequiredSuccessorCapabilities() {
    return SUCCESSOR_CAPABILITIES;
  }

  // --- The methods below, plus dependency on the Nekohtml parser
  // are only required because Nutch's summaries are in HTML by default.
  // I guess it would be possible to get rid of the code below by
  // adding patches/ methods to Nutch that return plain text summaries.
  // 
  // The temporary quick-and-dirty solution below has been provided by Doug, thanks. 

  /**
   * The text buffer for plain text. 
   */
  private StringBuffer textBuffer = new StringBuffer();
    
  /**
   * A parser that will convert html to plain text.
   */
  private AbstractSAXParser parser;

  /*
   * Anonymous initialization of the parser. Since we declared
   * the current solution to be quick and dirty, it doesn't have
   * to be in the constructor :)
   */
  {
    try {
      parser = new AbstractSAXParser(new HTMLConfiguration()){};
      parser.setContentHandler(new DefaultHandler() {
          public void characters(char[] chars, int start, int length)
            throws SAXException {
            textBuffer.append(chars, start, length);
          }
        });
    } catch (Exception e) {
      throw new RuntimeException(e.toString(), e);
    }
  }

  /**
   * Converts a html chunk to plain text.
   */
  private final String htmlToText(String html) {
    textBuffer.setLength(0);
    try {
      parser.parse(new InputSource(new StringReader(html)));
    } catch (Exception e) {                     // shouldn't happen
      throw new RuntimeException(e.toString(), e);
    }
    return textBuffer.toString();
  }
}
