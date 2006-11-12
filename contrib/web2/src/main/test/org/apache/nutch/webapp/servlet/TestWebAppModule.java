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

import java.io.ByteArrayInputStream;

import org.apache.nutch.webapp.common.WebAppModule;

import junit.framework.TestCase;

public class TestWebAppModule extends TestCase {

  public void testWebAppModule() {
    String webxml = "<web-app><servlet-mapping><servlet-name>a</servlet-name><url-pattern>/a.jsp</url-pattern></servlet-mapping>";
    webxml += "<servlet-mapping><servlet-name>b</servlet-name><url-pattern>/b.jsp</url-pattern></servlet-mapping>";
    webxml += "<servlet-mapping><servlet-name>c</servlet-name><url-pattern>/c.jsp</url-pattern></servlet-mapping></web-app>";

    WebAppModule module;
    module = new WebAppModule(new ByteArrayInputStream(webxml.getBytes()));
    assertEquals(3, module.getPaths().size());

    assertEquals("/a.jsp", module.getPaths().get(0));
    assertEquals("/b.jsp", module.getPaths().get(1));
    assertEquals("/c.jsp", module.getPaths().get(2));

    assertEquals("a", module.getServletName("/a.jsp"));
    assertEquals("b", module.getServletName("/b.jsp"));
    assertEquals("c", module.getServletName("/c.jsp"));
  }
}
