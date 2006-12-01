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
import java.io.InputStream;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nutch.plugin.PluginDescriptor;

/**
 * Simple Servlet that serves resources from plugins.
 *
 * Plugin must put resources to be exposed into path /resources/ for example
 * resource /resources/possible/sub/folder/resource.jpg will then be available
 * in location <contextPath>/resources/<plugin_id>/possible/sub/folder/resource.jpg
 *
 */
public class ResourceServlet extends NutchHttpServlet {

  private static final String PATH = "/resources/";

  private static final long serialVersionUID = 1L;

  /**
   * Extract plugin id from path.
   *
   * @param path
   * @return
   */
  private String getPluginId(final String path) {
    String pluginid = null;

    if (path != null && path.length() >= PATH.length()) {
      String stripped = path.substring(PATH.length());
      if (stripped.indexOf("/") != -1) {
        pluginid = stripped.substring(0, stripped.indexOf("/"));
      }
    }
    return pluginid;
  }

  /**
   * Extract plugin relative path from path.
   *
   * @param path
   * @return
   */
  private String getPluginRelativePath(final String path) {
    String resource = path.substring(PATH.length() + getPluginId(path).length()
        + 1);
    return resource;
  }

  /**
   * Process a request for resource inside plugin
   */
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    PluginDescriptor descriptor = null;
    String resPath = null;

    String path = getContextRelativePath(request);
    String id = getPluginId(path);
    if (id != null) {
      resPath = PATH.substring(1) + getPluginRelativePath(path);

      descriptor = getServiceLocator().getPluginRepository()
          .getPluginDescriptor(id);

      if (descriptor != null) {

        if (descriptor != null) {
          InputStream is = descriptor.getClassLoader().getResourceAsStream(
              resPath);
          if (is != null) {
            // buffer for content
            byte[] buffer = new byte[response.getBufferSize()];

            int len;
            String contentType = getServletContext().getMimeType(path)
                .toString();

            response.setContentType(contentType);
            while ((len = is.read(buffer)) != -1) {
              // copy to outputStream
              response.getOutputStream().write(buffer, 0, len);
            }
            is.close();
            response.flushBuffer();
            return;
          }
        }
      }
    }

    //of no resource was found dispay error
    response.setContentType("text/plain");
    PrintWriter pw = response.getWriter();
    pw.println("type:" + getServletContext().getMimeType(path));
    pw.println("path:" + path);
    pw.println("plugin-id:" + id);
    pw.println("plugin:" + descriptor);
    pw.println("plugin relative path:" + resPath);
    return;
  }
}
