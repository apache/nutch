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
package org.apache.nutch.webapp.controller;

import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.webapp.common.Search;
import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.struts.tiles.ComponentContext;

/* This is the main controller of nutch search application
 */
public class SearchController extends NutchController {
  
  public static final String REQ_ATTR_SEARCH="nutchSearch";
  public static final String REQ_ATTR_RESULTINFO="resultInfo";

  public void nutchPerform(ComponentContext tileContext, HttpServletRequest request,
      HttpServletResponse response, ServletContext servletContext)
      throws ServletException, IOException {
    
    ServiceLocator locator=getServiceLocator(request);
    
    Search search=locator.getSearch();
    request.setAttribute(REQ_ATTR_SEARCH, search);
    NutchBean bean = locator.getNutchBean();

    search.performSearch(bean);
    request.setAttribute(REQ_ATTR_RESULTINFO, search.getResultInfo());
  }
}
