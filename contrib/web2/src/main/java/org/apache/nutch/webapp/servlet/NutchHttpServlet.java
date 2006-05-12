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
package org.apache.nutch.webapp.servlet;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.nutch.webapp.common.ServletContextServiceLocator;

/**
 * Abstract base Servlet for nutch
 */
public abstract class NutchHttpServlet extends HttpServlet{

  private static final long serialVersionUID = 1L;

  private ServiceLocator locator;
  
  public void init(ServletConfig servletConfig) throws ServletException {
    locator = ServletContextServiceLocator.getInstance(servletConfig
        .getServletContext());
  }
  
  public ServiceLocator getServiceLocator(){
    return locator;
  }
}
