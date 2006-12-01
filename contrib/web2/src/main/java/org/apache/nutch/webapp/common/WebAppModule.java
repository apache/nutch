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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.lang.ClassLoader;

/**
 * This class is responsible for parsing web.xml definitions from plugins (put
 * there by jspc when precompiling jsp files).
 *
 * Extracted information is used at runtime when dispatching requests targetted
 * to jsp resources to the precompiled servlet classes.
 */
public class WebAppModule {

  HashMap servlets;

  List urlPatterns;

  ClassLoader loader = null;

  HashMap servletObjects = new HashMap();

  private ServletConfig servletConfig;

  /**
   * Construct new WebAppModule
   * 
   * @param input
   *          ionputstream to web.xml type of document
   */
  public WebAppModule(InputStream input) {
    loader = Thread.currentThread().getContextClassLoader();
    init(input);
  }

  public void init(InputStream input) {
    Document doc;
    servlets = new HashMap();
    urlPatterns = new ArrayList();

    if (input != null) {

      try {
        doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(
            input);
        parse(doc);
      } catch (SAXException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (ParserConfigurationException e) {
        e.printStackTrace();
      }
    }
    loadServlets();
  }

  public void loadServlets() {

    Iterator i = getPaths().iterator();

    while (i.hasNext()) {

      String key = (String) i.next();
      String servletName = (String) servlets.get(key);
      HttpServlet servletObject;
      Class servletClass;
      try {
        servletClass = loader.loadClass(servletName);
        servletObject = (HttpServlet) servletClass.newInstance();
        servletObject.init(servletConfig);
        servletObjects.put(key, servletObject);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      } catch (InstantiationException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      } catch (ServletException e) {
        e.printStackTrace();
      }
    }
  }

  public WebAppModule(ClassLoader loader, ServletConfig config) {
    servletConfig = config;
    this.loader = loader;
    init(loader.getResourceAsStream("META-INF/jsp-servlet-mappings.xml"));
  }

  /**
   * Get jsp paths available
   * 
   * @return
   */
  public List getPaths() {
    return urlPatterns;
  }

  public Map getMappings() {
    return servlets;
  }

  /**
   * Return Servlet name for path or null if non existing
   * 
   * @param path
   * @return
   */
  public String getServletName(String path) {
    return (String) servlets.get(path);
  }

  public void parse(Document doc) {

    Element root = doc.getDocumentElement();
    NodeList mappings = root.getElementsByTagName("servlet-mapping");
    for (int i = 0; i < mappings.getLength(); i++) {
      Element mapping = (Element) mappings.item(i);
      Element servlet = (Element) mapping.getElementsByTagName("servlet-name")
          .item(0);
      Element pattern = (Element) mapping.getElementsByTagName("url-pattern")
          .item(0);

      String servletName = servlet.getTextContent().trim();
      String urlPattern = pattern.getTextContent().trim();

      servlets.put(urlPattern, servletName);
      urlPatterns.add(urlPattern);
    }
  }

  /**
   * Dispatches request to precompiled jsp
   * 
   * @param relPath
   * @param request
   * @param response
   */
  public void dispatch(String relPath, HttpServletRequest request,
      HttpServletResponse response) {

    HttpServlet servletObject;
    if (servletObjects.containsKey(relPath)) {
      servletObject = (HttpServlet) servletObjects.get(relPath);
    } else {
      // servlet not found??
      return;
    }
    try {
      servletObject.service(request, response);
    } catch (ServletException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
