/**
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

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.OutputStream;
import java.io.IOException;

/**
 * A servlet that serves raw Content of any mime type.
 *
 * If it fails with java.lang.OutOfMemoryError, you may have to increase heap
 * size when starting container, such as -Xms1024M -Xmx1024M
 *
 * @author John Xing
 */
public class CachedServlet extends NutchHttpServlet {

  private static final long serialVersionUID = 1L;

  public void init(ServletConfig conf) throws ServletException {
    super.init(conf);
  }

  public void destroy() {
    // maybe clean bean?
    // nothing now
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {

    NutchBean.LOG.info("request from " + request.getRemoteAddr());

    Hit hit = new Hit(Integer.parseInt(request.getParameter("idx")), Integer
        .parseInt(request.getParameter("id")));
    HitDetails details = getServiceLocator().getNutchBean().getDetails(hit);

    // raw bytes
    byte[] bytes = getServiceLocator().getNutchBean().getContent(details);

    // pass all original headers? only these for now.
    Metadata metadata = getServiceLocator().getNutchBean()
        .getParseData(details).getContentMeta();
    String contentType = metadata.get(Response.CONTENT_TYPE);
    // String lastModified = metadata.get(Metadata.LAST_MODIFIED);
    // String contentLength = metadata.get(Metadata.CONTENT_LENGTH);
    // better use this, since it may have been truncated during fetch
    // or give warning if they don't match?
    int contentLength = bytes.length;

    // response
    response.setContentType(contentType);
    response.setContentLength(contentLength);

    OutputStream os = response.getOutputStream();
    os.write(bytes);
    // need this or flush more frequently?
    // os.flush();
    os.close();

    return;
  }

  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    doGet(request, response);
  }

}
