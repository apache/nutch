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

package org.apache.nutch.ontology;

import java.util.Iterator;

public interface Ontology {
  /** The name of the extension point. */
  public final static String X_POINT_ID = Ontology.class.getName();

  public void load(String[] urls);

  // not yet implemented
  //public void merge(Ontology o);

  public Iterator subclasses(String entitySearchTerm);

  public Iterator synonyms(String queryKeyPhrase);
}
