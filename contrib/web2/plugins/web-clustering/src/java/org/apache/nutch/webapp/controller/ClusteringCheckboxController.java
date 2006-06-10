package org.apache.nutch.webapp.controller;
import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nutch.clustering.ClusteringPresearchExtension;
import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.nutch.webapp.controller.NutchController;
import org.apache.struts.tiles.ComponentContext;

/**
 * Controller logic to decide wether clustering checkbox
 * is checked or not. 
 */
public class ClusteringCheckboxController extends NutchController {

  public static final String REQ_ATTR_CLUSTERING_ENABLED="clusteringEnabled";
  
  public void nutchPerform(ComponentContext tileContext,
      HttpServletRequest request, HttpServletResponse response,
      ServletContext servletContext) throws ServletException, IOException {
    
    ServiceLocator locator=getServiceLocator(request);
    
    if(ClusteringPresearchExtension.isClusteringActive(locator)) {
      request.setAttribute(REQ_ATTR_CLUSTERING_ENABLED, "1");
    }
  }
}
