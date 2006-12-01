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

import java.io.IOException;
import java.util.Locale;

import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.util.NutchConfiguration;

public abstract class ServletContextServiceLocator implements ServiceLocator {

  /**
   * ServiceLocator will be saved in ServletContext under this key
   */
  private static final String KEY = ServletContextServiceLocator.class
      .getName();

  /**
   * Logger.
   */
  public static final Log LOG = LogFactory
      .getLog(ServletContextServiceLocator.class);

  /**
   * ServletContext this ServiceLocator is bound to.
   */
  protected ServletContext servletContext;

  /**
   * Active configuration.
   */
  protected Configuration config;

  /**
   * Active PluginRepository.
   */
  private PluginRepository repository;

  /**
   * Active NutchBean.
   */
  private NutchBean bean;

  /**
   * Private Constructor used to create new ServiceLocator when needed.
   *
   * @param servletContext
   */
  private ServletContextServiceLocator(ServletContext servletContext) {
    this.servletContext = servletContext;
    config = NutchConfiguration.get(servletContext);
    repository = PluginRepository.get(config);
    try {
      bean = new NutchBean(config);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Factory method to get handle to ServiceLocator, or if none exists in
   * ServletContext then one is created.
   *
   * @param context
   * @return ServiceLocator instance bound to ServletContext
   */
  public static ServiceLocator getInstance(ServletContext context) {
    ServiceLocator locator = (ServiceLocator) context.getAttribute(KEY);
    if (locator == null) {
      LOG.info("creating new serviceLocator for context:" + context);
      locator = new ServletContextServiceLocator(context) {
        public SearchForm getSearchForm() {
          return null;
        }

        public Preferences getPreferences() {
          return null;
        }

        public Search getSearch() {
          return null;
        }

        public Locale getLocale() {
          return null;
        }
      };
      context.setAttribute(KEY, locator);
    }
    return locator;
  }

  public Configuration getConfiguration() {
    return config;
  }

  public PluginRepository getPluginRepository() {
    return repository;
  }

  public NutchBean getNutchBean() {
    return bean;
  }

  public PluginResourceLoader getPluginResourceLoader(ClassLoader loader) {
    return PluginResourceLoader.getInstance(this, loader);
  }

}
