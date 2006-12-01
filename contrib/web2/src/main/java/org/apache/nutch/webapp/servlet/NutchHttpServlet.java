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
package org.apache.nutch.webapp.servlet;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;

import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.nutch.webapp.common.ServletContextServiceLocator;

/**
 * Abstract base Servlet for nutch.
 */
public abstract class NutchHttpServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  private ServiceLocator locator;

  public void init(final ServletConfig servletConfig) throws ServletException {
    super.init(servletConfig);
    locator = ServletContextServiceLocator.getInstance(getServletContext());
  }

  /**
   * Return context relative path.
   *
   * @return
   */
  public String getContextRelativePath(final HttpServletRequest request) {
    return request.getRequestURI().substring(request.getContextPath().length());
  }

  /**
   * Removes the first part of path String and returns new String if there is no
   * path available return null.
   *
   * @param path
   *          original path
   * @return
   */
  public String getMappingRelativePath(final String path) {

    String relPath = null;

    if (path.indexOf("/", (path.startsWith("/") ? 1 : 0)) != -1) {
      relPath = path.substring(path
          .indexOf('/', (path.startsWith("/") ? 1 : 0)));
    }

    return relPath;
  }

  /**
   * Get ServiceLocator instance
   *
   * @return
   */
  public ServiceLocator getServiceLocator() {
    return locator;
  }
}
