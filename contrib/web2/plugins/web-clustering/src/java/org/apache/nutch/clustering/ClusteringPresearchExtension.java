/*
 * Copyright 2006 The Apache Software Foundation
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
package org.apache.nutch.clustering;

import org.apache.nutch.webapp.common.SearchContext;
import org.apache.nutch.webapp.extension.PreSearchExtensionPoint;

public class ClusteringPresearchExtension implements PreSearchExtensionPoint {

  /** 
   * This hook is executed before actual search
   * so we have a change to expand the result window
   * for clusterer
   */
  public void doPreSearch(SearchContext context) {
    System.out.println("Woohoo, executing presearch for clustering");
    int orig=context.getSearch().getHitsRequired();

    //TODO set this configurable
    if(orig < 100){
      context.getSearch().setHitsRequired(100);
    }
  }
}
