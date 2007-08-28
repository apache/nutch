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
package org.apache.nutch.clustering.carrot2;

import java.util.Map;
import java.util.Set;

import org.apache.nutch.searcher.HitDetails;
import org.carrot2.core.LocalInputComponentBase;
import org.carrot2.core.ProcessingException;
import org.carrot2.core.RequestContext;
import org.carrot2.core.clustering.RawDocumentsConsumer;
import org.carrot2.core.clustering.RawDocumentsProducer;

/**
 * An input component that ignores the query passed from the
 * controller and instead looks for data stored in the request context.
 * This enables us to reuse the same physical component implementation
 * for data that has already been acquired from Nutch.
 */
public class NutchInputComponent extends LocalInputComponentBase {
  public final static String NUTCH_INPUT_HIT_DETAILS_ARRAY
    = "NUTCH_INPUT_HIT_DETAILS_ARRAY";

  public final static String NUTCH_INPUT_SUMMARIES_ARRAY 
    = "NUTCH_INPUT_SUMMARIES_ARRAY";

  /** Capabilities required from the next component in the chain */
  private final static Set SUCCESSOR_CAPABILITIES = toSet(RawDocumentsConsumer.class);

  /** This component's capabilities */
  private final static Set COMPONENT_CAPABILITIES = toSet(RawDocumentsProducer.class);

  /**
   * Default language code for hits that don't have their own.
   */
  private String defaultLanguage;

  /**
   * Creates an input component with the given default language code.
   */
  public NutchInputComponent(String defaultLanguage) {
    this.defaultLanguage = defaultLanguage;
  }

  /*
   * @see com.dawidweiss.carrot.core.local.LocalInputComponent#setQuery(java.lang.String)
   */
  public void setQuery(String query) {
      // ignore the query; data will be provided from the request context.
  }

  /**
   * A callback hook that starts the processing.
   */
  public void startProcessing(RequestContext context) throws ProcessingException {
    // let successor components know that the processing has started.
    super.startProcessing(context);
    
    // get the information about documents from the context.
    final Map params = context.getRequestParameters();
    final HitDetails [] details = (HitDetails[]) params.get(NUTCH_INPUT_HIT_DETAILS_ARRAY);
    final String [] summaries = (String[]) params.get(NUTCH_INPUT_SUMMARIES_ARRAY);

    if (details == null)
      throw new ProcessingException("Details array must not be null.");

    if (summaries == null)
      throw new ProcessingException("Summaries array must not be null.");

    if (summaries.length != details.length)
      throw new ProcessingException("Summaries and details must be of the same length.");
    
    // produce 'documents' for successor components.
    final RawDocumentsConsumer consumer = (RawDocumentsConsumer) next;
    for (int i = 0; i < summaries.length; i++) {
      consumer.addDocument(new NutchDocument(i, details[i], summaries[i], defaultLanguage));
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
}
