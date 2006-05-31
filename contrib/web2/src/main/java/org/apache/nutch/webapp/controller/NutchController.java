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
package org.apache.nutch.webapp.controller;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.util.LogFormatter;
import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.nutch.webapp.common.WebappInstanceServiceLocator;
import org.apache.struts.tiles.ComponentContext;
import org.apache.struts.tiles.Controller;

/**
 *  Base class for nutch Tiles controllers.
 */
public abstract class NutchController implements Controller {

  public static Logger LOG = LogFormatter.getLogger(NutchController.class
      .getName());
  
  public final void execute(ComponentContext tileContext,
      HttpServletRequest request, HttpServletResponse response,
      ServletContext servletContext) throws ServletException, IOException {
    
    request.setCharacterEncoding("UTF-8");

    try {
      nutchPerform(tileContext, request, response, servletContext);
    } catch (Exception e) {
      LOG.info("Exception occured while executing nutch controller:");
      e.printStackTrace(System.err);
    }
  }
  
  /**
   * Nutch controllers overwrite this method
   * 
   * @param tileContext
   * @param request
   * @param response
   * @param servletContext
   * @throws ServletException
   * @throws IOException
   */
  public abstract void nutchPerform(ComponentContext tileContext,
      HttpServletRequest request, HttpServletResponse response,
      ServletContext servletContext) throws ServletException, IOException;

  /**
   * Get Active service locator from request or create on if not exiting
   * 
   * @param request
   * @return
   */
  protected ServiceLocator getServiceLocator(HttpServletRequest request) {
    return WebappInstanceServiceLocator.getFrom(request);
  }

  /**
   * Log request attributes. used for debugging
   * 
   * @param request
   */
  void logRequestAttributes(HttpServletRequest request) {
    Enumeration e = request.getAttributeNames();

    while (e.hasMoreElements()) {
      String name = (String) e.nextElement();
      LOG.info("request attrs:" + name + " = " + request.getAttribute(name));
    }
  }

  /**
   * Log tiles context. used for debugging
   * 
   * @param context
   */
  void logComponentContext(ComponentContext context) {
    Iterator i = context.getAttributeNames();

    while (i.hasNext()) {
      String name = (String) i.next();
      LOG.info("context attrs:" + name + " = " + context.getAttribute(name));
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.struts.tiles.Controller#perform(org.apache.struts.tiles.ComponentContext,
   *      javax.servlet.http.HttpServletRequest,
   *      javax.servlet.http.HttpServletResponse, javax.servlet.ServletContext)
   */
  public void perform(ComponentContext tileContext, HttpServletRequest request,
      HttpServletResponse response, ServletContext servletContext)
      throws ServletException, IOException {
    execute(tileContext, request, response, servletContext);
  }
}
