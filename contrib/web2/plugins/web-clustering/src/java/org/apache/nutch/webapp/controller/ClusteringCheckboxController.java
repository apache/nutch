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
