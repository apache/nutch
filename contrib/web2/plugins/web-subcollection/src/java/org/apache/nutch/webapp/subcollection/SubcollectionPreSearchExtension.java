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
