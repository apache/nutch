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

import org.apache.nutch.searcher.HitDetails;
import org.carrot2.core.clustering.RawDocument;
import org.carrot2.core.clustering.RawDocumentBase;

/**
 * An adapter class that implements {@link RawDocument} required for Carrot2.  
 */
public class NutchDocument extends RawDocumentBase {
  /**
   * Integer identifier of this document. We need a subclass of 
   * {@link java.lang.Object}, so this should do.
   */
  private final Integer id;
  
  /**
   * Creates a new document with the given id, <code>summary</code> and wrapping
   * a <code>details</code> hit details.
   */
  public NutchDocument(int id, HitDetails details, String summary, String defaultLanguage) {
    super(details.getValue("url"), details.getValue("title"), summary);

    // Handle document language -- attempt to extract it from the details,
    // otherwise set to the default.
    String lang = details.getValue("lang");
    if (lang == null) {
      // No default language. Take the default from the configuration file.
      lang = defaultLanguage;
    }

    // Use this language for the snippet. Truncate longer ISO codes
    // to only include two-letter language code.
    if (lang.length() > 2) {
      lang = lang.substring(0, 2);
    }
    lang = lang.toLowerCase();    
    super.setProperty(RawDocument.PROPERTY_LANGUAGE, lang);

    this.id = Integer.valueOf(id);
  }

  /*
   * @see com.dawidweiss.carrot.core.local.clustering.RawDocument#getId()
   */
  public Object getId() {
    return id;
  }
}