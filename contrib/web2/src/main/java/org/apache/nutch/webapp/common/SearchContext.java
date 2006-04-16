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

import org.apache.hadoop.conf.Configuration;

/**
 * SearchContext
 */
public interface SearchContext {

  /**
   * Get active search
   * 
   * @return
   */
  Search getSearch();

  /**
   * Test is named parameter exits
   * 
   * @param key
   * @return
   */
  boolean containsKey(Object key);

  /**
   * Return named parameter
   * 
   * @param key
   * @return
   */
  String getParameter(Object key);

  /**
   * Return named object
   * 
   * @param key
   * @return
   */
  Object get(Object key);

  /**
   * Return active Preferences
   * 
   * @return
   */
  Preferences getPreferences();

  /**
   * Return active SearchForm
   * 
   * @return
   */
  SearchForm getSearchForm();

  /**
   * Return active Configuration
   * 
   * @return
   */
  Configuration getConfigiration();

}
