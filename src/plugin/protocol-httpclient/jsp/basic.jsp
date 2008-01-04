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
  This JSP demonstrates basic authentication. When this JSP page is
  requested with no query parameters, then the user must enter the
  username as 'userx' and password as 'passx' when prompted for
  authentication. Apart from this there are a few other test cases,
  which can be used by passing a test case number as query parameter in
  the following manner: basic.jsp?case=1, basic.jsp?case=2, etc.
  The credentials for each test case can be easily figured out from the
  code below.

  Author: Susam Pal
--%>
<%@ page
    import = "sun.misc.BASE64Decoder"
%>
<%
  String authHeader = request.getHeader("Authorization");
  String realm = null;
  String username = null;
  String password = null;
  int testCase = 0;
  try {
    testCase = Integer.parseInt(request.getParameter("case"));
  } catch (Exception ex) {
    // do nothing
  }
  switch (testCase) {
    case 1:
      realm = "realm1"; username = "user1"; password = "pass1";
      break;

    case 2:
      realm = "realm2"; username = "user2"; password = "pass2";
      break;

    default:
      realm = "realmx"; username = "userx"; password = "passx";
      break;
  }

  boolean authenticated = false;
  if (authHeader != null && authHeader.toUpperCase().startsWith("BASIC")) {
    String creds[] = new String(new BASE64Decoder().decodeBuffer(
        authHeader.substring(6))).split(":", 2);
    if (creds[0].equals(username) && creds[1].equals(password))
          authenticated = true;
  }
  if (!authenticated) {
    response.setHeader("WWW-Authenticate", "Basic realm=\"" + realm + "\"");
    response.sendError(response.SC_UNAUTHORIZED);
  } else {
%>
<html>
<head><title>Basic Authentication Test</title></head>
<body>
<p>Hi <%= username %>, you have been successfully authenticated.</p>
</body>
</html>
<%
  }
%>
