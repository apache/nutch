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
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.LogFormatter;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.webapp.extension.PostSearchExtensionPoint;
import org.apache.nutch.webapp.extension.PreSearchExtensionPoint;
import org.apache.nutch.webapp.extension.SearchExtensionPoint;

public class BaseSearch {

  public static Logger LOG = LogFormatter.getLogger(BaseSearch.class.getName());

  protected PreSearchExtensionPoint[] presearch;

  protected SearchExtensionPoint[] search;

  protected PostSearchExtensionPoint[] postsearch;

  protected Object[] setup(String xPoint, Configuration conf) {
    HashMap filters = new HashMap();
    try {
      ExtensionPoint point = serviceLocator.getPluginRepository()
          .getExtensionPoint(xPoint);
      if (point == null)
        throw new RuntimeException(xPoint + " not found.");
      Extension[] extensions = point.getExtensions();
      for (int i = 0; i < extensions.length; i++) {
        Extension extension = extensions[i];
        Object extensionInstance = extension.getExtensionInstance();
        if (!filters.containsKey(extensionInstance.getClass().getName())) {
          filters
              .put(extensionInstance.getClass().getName(), extensionInstance);
        }
      }
      return (Object[]) filters.values().toArray(new Object[filters.size()]);
    } catch (PluginRuntimeException e) {
      throw new RuntimeException(e);
    }
  }

  // private HttpServletRequest request;
  private SearchContextImpl context;

  private ServiceLocator serviceLocator;

  /**
   * Construct new BaseSearch object
   */
  public BaseSearch(ServiceLocator locator) {
    this.serviceLocator = locator;
    presearch = getPreSearchExtensions(serviceLocator.getConfiguration());
    search = getSearchExtensions(serviceLocator.getConfiguration());
    postsearch = getPostSearchExtensions(serviceLocator.getConfiguration());
  }

  public PreSearchExtensionPoint[] getPreSearchExtensions(Configuration conf) {
    if (conf.getObject(PreSearchExtensionPoint.X_POINT_ID) == null) {
      conf.set(PreSearchExtensionPoint.X_POINT_ID, setup(
          PreSearchExtensionPoint.X_POINT_ID, conf));
    }
    return (PreSearchExtensionPoint[]) conf
        .getObject(PreSearchExtensionPoint.X_POINT_ID);
  }

  public SearchExtensionPoint[] getSearchExtensions(Configuration conf) {
    if (conf.getObject(SearchExtensionPoint.X_POINT_ID) == null) {
      conf.set(SearchExtensionPoint.X_POINT_ID, setup(
          SearchExtensionPoint.X_POINT_ID, conf));
    }
    return (SearchExtensionPoint[]) conf
        .getObject(SearchExtensionPoint.X_POINT_ID);
  }

  public PostSearchExtensionPoint[] getPostSearchExtensions(Configuration conf) {
    if (conf.getObject(PostSearchExtensionPoint.X_POINT_ID) == null) {
      conf.set(PostSearchExtensionPoint.X_POINT_ID, setup(
          PostSearchExtensionPoint.X_POINT_ID, conf));
    }
    return (PostSearchExtensionPoint[]) conf
        .getObject(PostSearchExtensionPoint.X_POINT_ID);
  }

  /**
   * Call plugins participating PreSearch activities
   */
  void callPreSearch() {
    for (int i = 0; i < presearch.length; i++) {
      presearch[i].doPreSearch(context);
    }
  }

  /**
   * Call plugins participating Search activities
   */
  void callSearch() {
    for (int i = 0; i < search.length; i++) {
      search[i].doSearch(context);
    }
  }

  /**
   * Call plugins participating postSearch activities
   */
  void callPostSearch() {
    for (int i = 0; i < postsearch.length; i++) {
      postsearch[i].doPostSearch(context);
    }
  }

  /**
   * Entry point to execute the search
   */
  public void doSearch() {
    // initiate Search Object
    LOG.fine("create search");
    Search search = new Search(serviceLocator);
    // create context
    LOG.fine("create context");
    context = new SearchContextImpl(search, serviceLocator);
    LOG.fine("presearch");
    callPreSearch();
    LOG.fine("search");
    callSearch();
    LOG.fine("post");
    callPostSearch();
  }
}
