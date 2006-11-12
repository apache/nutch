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
package org.apache.nutch.clustering;

import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.nutch.webapp.extension.PreSearchExtensionPoint;

/**
 * This class is responsible for interpreting request parameters
 * and reacting if named parameter is defined.
 * 
 * If clustering is available the result window is checked
 * and expanded to 100 hits if required.
 */
public class ClusteringPresearchExtension implements PreSearchExtensionPoint {


  /**
   * Check wether clustering is active or not
   * @param locator
   * @return true if clustering is active
   */
  public static boolean isClusteringActive(ServiceLocator locator){
    return locator.getSearchForm().getValueString(REQ_PARAM_CLUSTERING_ENABLED)!=null;
  }
  
  /**
   * The parameter name to be searched from request
   */
  public static final String REQ_PARAM_CLUSTERING_ENABLED="clustering";

  /* 
   * This hook is executed before actual search
   * so we have a change to expand the result window
   * for clusterer if clustering is active for the request
   */
  public void doPreSearch(ServiceLocator locator) {

    if(isClusteringActive(locator)) {

      int orig=locator.getSearch().getHitsRequired();

      int hitsToCluster = locator.getConfiguration().getInt(
        "extension.clustering.hits-to-cluster", 100);
    
      if(orig < hitsToCluster){
        locator.getSearch().setHitsRequired(hitsToCluster);
      }
    }
  }
}
