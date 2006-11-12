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
import java.util.List;
import java.util.ArrayList;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nutch.html.Entities;
import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.struts.tiles.ComponentContext;

public class AnchorsController extends NutchController {

  public class AnchorsBean {
    HitDetails details;

    ArrayList anchors;

    protected AnchorsBean(ArrayList v, HitDetails details) {
      this.anchors = v;
      this.details = details;
    }

    public String getUrl() {
      return details.getValue("url");
    }

    public List getAnchors() {
      return anchors;
    }
  }

  public void nutchPerform(ComponentContext tileContext,
      HttpServletRequest request, HttpServletResponse response,
      ServletContext servletContext) throws ServletException, IOException {
    ServiceLocator locator = getServiceLocator(request);
    NutchBean bean = locator.getNutchBean();

    LOG.info("anchors request from " + request.getRemoteAddr());
    Hit hit = new Hit(Integer.parseInt(request.getParameter("idx")), Integer
        .parseInt(request.getParameter("id")));

    HitDetails details = bean.getDetails(hit);

    String[] anchors = bean.getAnchors(details);
    ArrayList anchorVector = new ArrayList();
    if (anchors != null) {
      for (int i = 0; i < anchors.length; i++) {
        anchorVector.add(Entities.encode(anchors[i]));
      }
    }
    request
        .setAttribute("nutchAnchors", new AnchorsBean(anchorVector, details));
    logRequestAttributes(request);
  }
}
