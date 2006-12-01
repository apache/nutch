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

import org.apache.hadoop.conf.Configuration;

import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.searcher.NutchBean;

/**
 * ServiceLocator interface. ServiceLocator is used to get handle to some of the
 * core services of nutch.
 */
public interface ServiceLocator {

  /**
   * Return active Configuration instance (application context).
   *
   * @return
   */
  public Configuration getConfiguration();

  /**
   * Return active PluginRepsository instance (application instance).
   *
   * @return
   */
  public PluginRepository getPluginRepository();

  /**
   * Return active SearchForm (request instance).
   *
   * @return
   */
  public SearchForm getSearchForm();

  /**
   * Return active Preferences (request context).
   *
   * @return
   */
  public Preferences getPreferences();

  /**
   * Return active NutchBean (application context).
   *
   * @return
   */
  public NutchBean getNutchBean();

  /**
   * Return active PluginResourceLoader (application context).
   *
   * @return
   */
  public PluginResourceLoader getPluginResourceLoader(ClassLoader loader);

  /**
   * Return active Search (request context).
   *
   * @return
   */
  public Search getSearch();

  /**
   * Return active locale
   * @return
   */
  public Locale getLocale();

}
