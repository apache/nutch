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

import org.apache.nutch.ontology.Ontology;
import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.nutch.webapp.common.Startable;
import org.apache.struts.tiles.ComponentContext;

public class OntologyProposerController extends NutchController implements Startable {

  public static final String ATTR_NAME="queryPropose";
  
  static Ontology ontology;
  
  public void nutchPerform(ComponentContext tileContext, HttpServletRequest request, HttpServletResponse response, ServletContext servletContext) throws ServletException, IOException {

    ServiceLocator locator=getServiceLocator(request);

    if (ontology != null) {
      LOG.info("Calling ontology with parameter:" + locator.getSearch().getQueryString());
      request.setAttribute(ATTR_NAME, ontology.subclasses(locator.getSearch().getQueryString()));
    }
  }

  /*
   *  (non-Javadoc)
   * @see org.apache.nutch.webapp.common.Startable#start(javax.servlet.ServletContext)
   */
  public void start(ServletContext servletContext) {
    
    ServiceLocator locator=getServiceLocator(servletContext);
    
    try {
      String urls = locator.getConfiguration().get("extension.ontology.urls");
      ontology = new org.apache.nutch.ontology.OntologyFactory(locator.getConfiguration()).getOntology();
      
      LOG.info("Initializing Ontology with urls:" + urls);

      //FIXME initialization code should be in Ontology instead
      if (urls==null || urls.trim().equals("")) {
        // ignored siliently
      } else {
        ontology.load(urls.split("\\s+"));
      }
    } catch (Exception e) {
      LOG.info("Exception occured while initializing Ontology" + e);
    }
  }
}
