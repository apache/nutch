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
package org.apache.nutch.webapp.tiles;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.jsp.jstl.core.Config;

import org.apache.nutch.webapp.common.ServiceLocator;
import org.apache.nutch.webapp.common.WebappInstanceServiceLocator;
import org.apache.struts.tiles.TilesRequestProcessor;

public class NutchRequestProcessor extends TilesRequestProcessor {


  /**
   * Process locale, override the default functionality and prevent
   * session from being created.
   */
  protected void processLocale(HttpServletRequest request,
      HttpServletResponse response) {

    ServiceLocator locator = WebappInstanceServiceLocator.getFrom(request);

    if (locator == null) {
      locator = new WebappInstanceServiceLocator(request, getServletContext());
      WebappInstanceServiceLocator.register(request,
          (WebappInstanceServiceLocator) locator);
    }

    Config.set(request, Config.FMT_LOCALE, locator.getLocale());
  }
}
