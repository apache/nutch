<%--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>
<%--
  This JSP tests whether the client can remember cookies. When the JSP
  is fetched for the first time without any query parameters, it sets
  a few cookies in the client. On a second request, with the query
  parameter, 'cookie=yes', it checks whether all the client has sent
  the cookies. If the cookies are found, HTTP 200 response is returned.
  If the cookies are not found, HTTP 403 response is returned.

  Author: Susam Pal
--%>
<%
  String cookieParam = request.getParameter("cookie");
  if (!"yes".equals(cookieParam)) { // Send cookies
    response.addCookie(new Cookie("var1", "val1"));
    response.addCookie(new Cookie("var2", "val2"));
%>
<html>
<head><title>Cookies Set</title></head>
<body><p>Cookies have been set.</p></body>
</html>
<%
  } else { // Check cookies
    int cookiesCount = 0;

    Cookie[] cookies = request.getCookies();
    if (cookies != null) {
      for (int i = 0; i < cookies.length; i++) {
        if (cookies[i].getName().equals("var1")
            && cookies[i].getValue().equals("val1"))
          cookiesCount++;

        if (cookies[i].getName().equals("var2")
            && cookies[i].getValue().equals("val2"))
          cookiesCount++;
      }
    }

    if (cookiesCount != 2) {
      response.sendError(response.SC_FORBIDDEN);
    } else {
%>
<html>
<head><title>Cookies Found</title></head>
<body><p>Cookies found!</p></body>
</html>
<%
    }
  }
%>
