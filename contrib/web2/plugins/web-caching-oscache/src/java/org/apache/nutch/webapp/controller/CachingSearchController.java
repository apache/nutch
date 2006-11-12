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

import org.apache.nutch.webapp.CacheManager;
import org.apache.nutch.webapp.common.Search;
import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.nutch.webapp.common.Startable;
import org.apache.nutch.webapp.controller.SearchController;
import org.apache.struts.tiles.ComponentContext;

import com.opensymphony.oscache.base.NeedsRefreshException;

/**
 * This naive search result caching implementation is just an example of
 * extending the web ui.
 */
public class CachingSearchController extends SearchController implements Startable {

  CacheManager manager=null;

  public void nutchPerform(ComponentContext tileContext,
      HttpServletRequest request, HttpServletResponse response,
      ServletContext servletContext) throws ServletException, IOException {

    ServiceLocator locator = getServiceLocator(request);
    Search search;
    
    // key used for caching results, should really be something else but a part of user
    // definable String
    String key = request.getQueryString().replace("?","_").replace("&","_");
    StringBuffer cacheKey=new StringBuffer(key.length()*2);
    for(int i=0;i<key.length();i++){
      cacheKey.append(key.charAt(i)).append(java.io.File.separatorChar);
    }
    
    if(LOG.isDebugEnabled()){
      LOG.debug("cache key:" + cacheKey);
    }
    if (cacheKey != null) {
      try {
        search = manager.getSearch(cacheKey.toString(), locator);
        request.setAttribute(Search.REQ_ATTR_SEARCH, search);
        if(LOG.isDebugEnabled()) {
          LOG.debug("Using cached");
        }
      } catch (NeedsRefreshException e) {
        try{
          super.nutchPerform(tileContext, request, response, servletContext);
          search = (Search) locator.getSearch();
          manager.putSearch(cacheKey.toString(),
            search);
        } catch (Exception ex){
          LOG.info("Cancelling update");
          manager.cancelUpdate(cacheKey.toString());
        }
      }
    }
  }

  public void start(ServletContext servletContext) {
    ServiceLocator locator=getServiceLocator(servletContext);
    manager=CacheManager.getInstance(locator.getConfiguration());
  }
}
