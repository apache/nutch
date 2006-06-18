package org.apache.nutch.webapp.controller;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nutch.collection.CollectionManager;
import org.apache.nutch.collection.Subcollection;
import org.apache.nutch.indexer.subcollection.SubcollectionIndexingFilter;
import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.nutch.webapp.common.Startable;
import org.apache.nutch.webapp.controller.NutchController;
import org.apache.struts.tiles.ComponentContext;

/**
 * This controller is responsible for providing Collection of
 * subcollections for displaying. 
 */
public class SubcollectionSelectController extends NutchController implements Startable {

  public class SubcollectionWrapper {
    Subcollection col;
    boolean checked;
    
    /**
     * UI Wrapper for subcollection 
     * @param col Subcollection
     * @param checked is the subcollection "checked" / active
     */
    public SubcollectionWrapper(Subcollection col, boolean checked){
      this.col=col;
      this.checked=checked;
    }
    
    public String getId(){
      return col.getId();
    }

    public String getName(){
      return col.getName();
    }

    public boolean getChecked(){
      return checked;
    }
}
  
  public static final String REQ_ATTR_SUBCOLLECTIONS="subcollections";
  public static final String REQ_ATTR_COLLECTION_SELECTED="iscollectionlimited";
  
  private CollectionManager collectionManager;
  
  public void nutchPerform(ComponentContext tileContext,
      HttpServletRequest request, HttpServletResponse response,
      ServletContext servletContext) throws ServletException, IOException {
    
    boolean hasSelectedCollection=false;
    ServiceLocator serviceLocator = getServiceLocator(request);
    Collection collections=collectionManager.getAll();
    
    ArrayList wrapped=new ArrayList();
    Iterator i=collections.iterator();
    
    String value=serviceLocator.getSearchForm().getValueString(SubcollectionIndexingFilter.FIELD_NAME);
    
    while(i.hasNext()){
      Subcollection collection=(Subcollection)i.next();
      boolean checked=(value!=null && value.equals(collection.getId()));
      wrapped.add(new SubcollectionWrapper(collection,checked));
      if(checked){
        hasSelectedCollection=true;
      }
    }
    
    request.setAttribute(REQ_ATTR_SUBCOLLECTIONS, wrapped);
    if(hasSelectedCollection){
      LOG.info("collection was selected");
      request.setAttribute(REQ_ATTR_COLLECTION_SELECTED,"1");
    }
  }

  public void start(ServletContext servletContext) {
    ServiceLocator serviceLocator=getServiceLocator(servletContext);
    collectionManager=CollectionManager.getCollectionManager(serviceLocator.getConfiguration());
  }

}
