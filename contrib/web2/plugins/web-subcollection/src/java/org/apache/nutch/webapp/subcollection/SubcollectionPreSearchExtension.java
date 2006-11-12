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
package org.apache.nutch.webapp.subcollection;

import org.apache.nutch.indexer.subcollection.SubcollectionIndexingFilter;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.nutch.webapp.extension.PreSearchExtensionPoint;

/**
 * This class is responsible for limiting search to certain subcollection.
 * Subcollection to be searched is defined with queryparameter named
 * "subcollection".
 */
public class SubcollectionPreSearchExtension implements PreSearchExtensionPoint {

  public void doPreSearch(ServiceLocator locator) {
    Query original=locator.getSearch().getQuery();
    Query modified=(Query)original.clone();
    try{
      String value=locator.getSearchForm().getValueString(SubcollectionIndexingFilter.FIELD_NAME);
      if(value!=null && value.trim().length()>0) {
        modified.addRequiredTerm(value, SubcollectionIndexingFilter.FIELD_NAME);
      }
    } catch (Exception e){
      
    }
    locator.getSearch().setQuery(modified);
  }

}
