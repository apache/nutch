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

import org.apache.nutch.searcher.HitDetails;

import com.dawidweiss.carrot.core.local.clustering.RawDocument;
import com.dawidweiss.carrot.core.local.clustering.RawDocumentBase;

/**
 * An adapter class that implements {@link RawDocument} for
 * Carrot2.  
 *
 * @author Dawid Weiss
 * @version $Id: NutchDocument.java,v 1.2 2004/08/10 00:18:43 johnnx Exp $
 */
public class NutchDocument extends RawDocumentBase {

  private final Integer id;
  
  /**
   * Creates a new document with the given id, <code>summary</code> and wrapping
   * a <code>details</code> hit details.
   */
  public NutchDocument(int id, HitDetails details, String summary) {
    super.setProperty(RawDocument.PROPERTY_URL, details.getValue("url"));
    super.setProperty(RawDocument.PROPERTY_SNIPPET, summary);
    
    String title = details.getValue("title");
    if (title != null && !"".equals(title)) {
      super.setProperty(RawDocument.PROPERTY_TITLE, title);
    }
    
    this.id = new Integer(id);
  }
  
  /*
   * @see com.dawidweiss.carrot.core.local.clustering.RawDocument#getId()
   */
  public Object getId() {
    return id;
  }
}
