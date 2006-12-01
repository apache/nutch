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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.ExtensionPoint;
import org.apache.nutch.webapp.common.WebAppModule;
import org.apache.nutch.webapp.extension.UIExtensionPoint;

/**
 * This Servlet is responsible for dispatching requests to jsp resources from
 * plugins.
 */
public class JspDispatcherServlet extends NutchHttpServlet {

  private static final long serialVersionUID = 1L;

  HashMap allMappings = new HashMap();

  public void init(ServletConfig conf) throws ServletException {
    super.init(conf);

    // TODO should perhaps first try handle core definitions

    // process extensions
    ExtensionPoint point = getServiceLocator().getPluginRepository()
        .getExtensionPoint(UIExtensionPoint.X_POINT_ID);

    if (point != null) {

      Extension[] extensions = point.getExtensions();

      for (int i = 0; i < extensions.length; i++) {
        Extension extension = extensions[i];

        WebAppModule module = new WebAppModule(extension.getDescriptor()
            .getClassLoader(), conf);
        Iterator iterator = module.getPaths().iterator();

        while (iterator.hasNext()) {
          String path = (String) iterator.next();
          allMappings.put(path, module);
        }
      }
    }
  }

  /** 
   * Process request to jsp inside plugin (jsps must be precompiled)
   * 
   */
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    //get jsp include path_info from request object
    String relPath=(String)request.getAttribute("javax.servlet.include.path_info");

    if (allMappings.containsKey(relPath)) {
      WebAppModule module=(WebAppModule)allMappings.get(relPath);
      module.dispatch(relPath, request,response);
    }
  }
}
