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

import java.io.IOException;
import java.util.HashMap;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nutch.keymatch.CountFilter;
import org.apache.nutch.keymatch.KeyMatch;
import org.apache.nutch.keymatch.SimpleKeyMatcher;
import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.nutch.webapp.common.Startable;
import org.apache.struts.tiles.ComponentContext;

public class KeyMatchController extends NutchController implements Startable{

  public static final String ATTR_KEYMATCHES="keymatches";
  
  static SimpleKeyMatcher keymatcher;
  static HashMap context;
  
  public void nutchPerform(ComponentContext tileContext,
      HttpServletRequest request, HttpServletResponse response,
      ServletContext servletContext) throws ServletException, IOException {
    ServiceLocator serviceLocator=getServiceLocator(request);
    HashMap context=new HashMap();
    KeyMatch[] matches=keymatcher.getMatches(serviceLocator.getSearch().getQuery(),context);
    request.setAttribute(ATTR_KEYMATCHES, matches);
  }

  public void start(ServletContext servletContext) {
    LOG.info("Starting keymatcher");
    ServiceLocator serviceLocator=getServiceLocator(servletContext);
    keymatcher=new SimpleKeyMatcher(serviceLocator.getConfiguration());
    context=new HashMap();
    //how many matches
    context.put(CountFilter.KEY_COUNT,"1");
    LOG.info("Starting keymatcher ok");
  }
}
