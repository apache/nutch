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
import java.util.Date;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nutch.webapp.common.SearchResultBean;
import org.apache.struts.tiles.ComponentContext;

/**
 * This is the controller used when rendering a hit. Basically what
 * happens here is that we get some meta data values from HitDetails
 * and stick them into request object so the jsp can display them.
 * 
 */
public class MoreController extends NutchController {

  public void nutchPerform(ComponentContext tileContext,
      HttpServletRequest request, HttpServletResponse response,
      ServletContext servletContext) throws ServletException, IOException {

    SearchResultBean hit = (SearchResultBean) request.getAttribute("hit");

    if (hit != null) {

      // Content-Type
      String primaryType = hit.getDetails().getValue("primaryType");
      String subType = hit.getDetails().getValue("subType");

      String contentType = subType;
      if (contentType == null)
        contentType = primaryType;
      if (contentType != null) {
        request.setAttribute("contentType", contentType);
      }
      // Content-Length
      String contentLength = hit.getDetails().getValue("contentLength");
      if (contentLength != null) {
        request.setAttribute("contentLength", contentLength);
      }

      // Last-Modified
      String lastModified = hit.getDetails().getValue("lastModified");
      if (lastModified != null) {
        long millis = new Long(lastModified).longValue();
        Date date = new Date(millis);
        request.setAttribute("lastModified", date);
      }
    } else {
      LOG.info("hit was null?");
    }
  }
}
