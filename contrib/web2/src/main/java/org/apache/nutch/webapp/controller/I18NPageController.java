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
package org.apache.nutch.webapp.controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Locale;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.struts.tiles.ComponentContext;

/**
 * This controller can be used in pages that require displaying of localized
 * content. In nutch this means help etc. pages.
 */
public class I18NPageController extends NutchController {

  protected HashMap cached = new HashMap();

  public void nutchPerform(ComponentContext tileContext,
      HttpServletRequest request, HttpServletResponse response,
      ServletContext servletContext) throws ServletException, IOException {

    ServiceLocator locator = getServiceLocator(request);

    // get base page from tiles context
    String baseName = (String) tileContext.getAttribute("basePage");

    // get arequest attribute name (where content will be put)
    // from tiles context
    String attrName = (String) tileContext.getAttribute("attrName");

    // if not available use default
    if (attrName == null) {
      attrName = "content";
    }

    // get users preferred locale
    Locale locale = locator.getLocale();

    // see if we have a cached content available
    StringBuffer content = (StringBuffer) cached.get(baseName + locale);

    // not available, try to read from file
    // XXX this prevents plugins to provide content
    if (content == null) {
      content = new StringBuffer();
      List suffixes = calculateSuffixes(locale);

      if (!suffixes.contains(servletContext
          .getInitParameter("javax.servlet.jsp.jstl.fmt.fallbackLocale"))) {
        suffixes.add(servletContext
            .getInitParameter("javax.servlet.jsp.jstl.fmt.fallbackLocale"));
      }

      Iterator iterator = suffixes.iterator();

      while (iterator.hasNext()) {
        String postfix = (String) iterator.next();
        String name = concatPostfix(baseName, postfix);

        InputStream is = servletContext.getResourceAsStream(name);
        if (is != null) {
          BufferedReader br = new BufferedReader(new InputStreamReader(is));

          String line;
          while ((line = br.readLine()) != null) {
            content.append(line).append("\n");
          }
          br.close();
          is.close();
          break;
        }
      }

      // put in cache for faster retrieval later
      cached.put(baseName + locale, content);

    }
    request.setAttribute(attrName, content.toString());
  }

  /**
   * Calculate the suffixes based on the locale.
   * 
   * @param locale
   *          the locale
   * 
   * This method is borrowed from I18NFactorySet.java wich is part of tiles
   * (struts)
   * 
   */
  private List calculateSuffixes(Locale locale) {

    List suffixes = new ArrayList(3);
    String language = locale.getLanguage();
    String country = locale.getCountry();
    String variant = locale.getVariant();

    StringBuffer suffix = new StringBuffer();
    // suffix.append('_');
    suffix.append(language);
    if (language.length() > 0) {
      suffixes.add(suffix.toString());
    }

    suffix.append('_');
    suffix.append(country);
    if (country.length() > 0) {
      suffixes.add(suffix.toString());
    }

    suffix.append('_');
    suffix.append(variant);
    if (variant.length() > 0) {
      suffixes.add(suffix.toString());
    }

    return suffixes;
  }

  /**
   * Concat postfix to the name. Take care of existing filename extension.
   * Transform the given name "name.ext" to have "name" + "postfix" + "ext". If
   * there is no ext, return "name" + "postfix".
   *
   * @param name
   *          Filename.
   * @param postfix
   *          Postfix to add.
   * @return Concatenated filename.
   *
   * This method is borrowed from I18NFactorySet.java wich is part of tiles
   * (struts)
   */
  private String concatPostfix(String name, String postfix) {
    if (postfix == null) {
      return name;
    }
    return "/" + postfix + name;
  }

}
