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
import java.io.UnsupportedEncodingException;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.struts.tiles.ComponentContext;

public class CachedController extends NutchController {

  public void nutchPerform(ComponentContext tileContext,
      HttpServletRequest request, HttpServletResponse response,
      ServletContext servletContext) throws ServletException, IOException {

    ServiceLocator locator = getServiceLocator(request);
    NutchBean bean = locator.getNutchBean();

    LOG.info("Cache request from " + request.getRemoteAddr());

    Hit hit = new Hit(Integer.parseInt(request.getParameter("idx")),
                      Integer.parseInt(request.getParameter("id")));

    HitDetails details = bean.getDetails(hit);
    String id = "idx=" + hit.getIndexNo() + "&id=" + hit.getIndexDocNo();

    Metadata metaData = bean.getParseData(details).getContentMeta();

    String content = null;
    String contentType = (String) metaData.get(Metadata.CONTENT_TYPE);


    if (contentType.startsWith("text/html")) {
      // FIXME : it's better to emit the original 'byte' sequence
      // with 'charset' set to the value of 'CharEncoding',
      // but I don't know how to emit 'byte sequence' in JSP.
      // out.getOutputStream().write(bean.getContent(details)) may work,
      // but I'm not sure.
      String encoding = (String) metaData.get("CharEncodingForConversion");
      if (encoding != null) {
        try {
          content = new String(bean.getContent(details), encoding);
        } catch (UnsupportedEncodingException e) {
          //fallback to configured charset
          content = new String(bean.getContent(details), locator
              .getConfiguration().get("parser.character.encoding.default"));
        }
      } else {
        //construct String with system default encoding
        content = new String(bean.getContent(details));
      }
    }

    // page content
    request.setAttribute("content", content);
    // page content type
    request.setAttribute("contentType", contentType);
    // page url
    request.setAttribute("url", details.getValue("url"));
    // page id
    request.setAttribute("id", id);
    // page content if html
    request.setAttribute("isHtml", new Boolean(contentType
        .startsWith("text/html")));
  }
}
