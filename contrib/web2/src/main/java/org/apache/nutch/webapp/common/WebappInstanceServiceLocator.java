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
package org.apache.nutch.webapp.common;

import java.util.Locale;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.searcher.NutchBean;

public class WebappInstanceServiceLocator implements ServiceLocator {

  public static final Log LOG = LogFactory.getLog(WebappInstanceServiceLocator.class);

  HttpServletRequest request;

  ServiceLocator contextLocator;

  /**
   * Creates new WebappInstanceServiceLocator instance and binds it to request
   * 
   * @param request
   * @param servletContext
   */
  public WebappInstanceServiceLocator(HttpServletRequest request,
      ServletContext servletContext) {
    this.request = request;
    contextLocator = ServletContextServiceLocator.getInstance(servletContext);
    WebappInstanceServiceLocator.register(request, this);
  }

  public Preferences getPreferences() {
    return Preferences.getPreferences(request);
  }

  public SearchForm getSearchForm() {
    if (request.getAttribute(SearchForm.class.getName()) == null) {
      request.setAttribute(SearchForm.class.getName(), new SearchForm(request
          .getParameterMap()));
    }
    return (SearchForm) request.getAttribute(SearchForm.class.getName());
  }

  public Search getSearch() {
    String key = Search.REQ_ATTR_SEARCH;
    Search search = (Search) request.getAttribute(key);
    if (search == null) {
      search = new Search(this);
      request.setAttribute(key, search);
    }

    return search;
  }

  /**
   * @param request
   * @return
   */
  public static ServiceLocator getFrom(HttpServletRequest request) {
    return (ServiceLocator) request
        .getAttribute(WebappInstanceServiceLocator.class.getName());
  }

  /**
   * @param request
   * @param locator
   */
  public static void register(HttpServletRequest request,
      WebappInstanceServiceLocator locator) {
    WebappInstanceServiceLocator l = (WebappInstanceServiceLocator) request
        .getAttribute(WebappInstanceServiceLocator.class.getName());
    if (locator != null && locator != l) {
      request.setAttribute(WebappInstanceServiceLocator.class.getName(),
          locator);
    }
  }

  public Configuration getConfiguration() {
    return contextLocator.getConfiguration();
  }

  public PluginRepository getPluginRepository() {
    return contextLocator.getPluginRepository();
  }

  public NutchBean getNutchBean() {
    return contextLocator.getNutchBean();
  }

  public PluginResourceLoader getPluginResourceLoader(ClassLoader loader) {
    return contextLocator.getPluginResourceLoader(loader);
  }

  public Locale getLocale() {
    return getPreferences().getLocale(request);
  }

}
