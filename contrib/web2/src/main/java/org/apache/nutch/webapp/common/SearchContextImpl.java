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
package org.apache.nutch.webapp.common;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

/**
 * Default Implementation of
 * 
 * @see SearchContext
 */
public class SearchContextImpl extends HashMap implements SearchContext {

  private static final long serialVersionUID = 1L;

  Search search;

  ServiceLocator serviceLocator;

  /**
   * Constructs a SearchContextImpl object and initializes it with provided data
   * 
   * @param search
   *          the Search object this context belongs to
   * @param locator
   *          Used ServiceLocator
   */
  SearchContextImpl(Search search, ServiceLocator locator) {
    super();
    this.search = search;
    this.serviceLocator = locator;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.nutch.webapp.common.SearchContext#getSearch()
   */
  public Search getSearch() {
    return search;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.nutch.webapp.common.SearchContext#getParameter(java.lang.Object)
   */
  public String getParameter(Object key) {
    return (String) get(key);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.nutch.webapp.common.SearchContext#getPreferences()
   */
  public Preferences getPreferences() {
    return serviceLocator.getPreferences();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.nutch.webapp.common.SearchContext#getSearchForm()
   */
  public SearchForm getSearchForm() {
    return serviceLocator.getSearchForm();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.nutch.webapp.common.SearchContext#getConfigiration()
   */
  public Configuration getConfigiration() {
    return serviceLocator.getConfiguration();
  }
}
