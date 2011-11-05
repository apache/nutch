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
  This JSP tests digest authentication. It generates an HTTP response
  with authorization header for digest authentication and checks the
  user-name supplied by the client. It does not check the other
  parameters and hashes as controlled JUnit tests would be performed
  against this and only the proper submission of credentials need to
  be tested.

  Author: Susam Pal
--%>
<%@ page
    import = "java.util.StringTokenizer"
    import = "java.util.HashMap"
%>
<%
  String username = "digest_user";
  String authHeader = request.getHeader("Authorization");
  
  boolean authenticated = false;
  if (authHeader != null && authHeader.toUpperCase().startsWith("DIGEST")) {
    HashMap map = new HashMap();
    StringTokenizer tokenizer = new StringTokenizer(
        authHeader.substring(7).trim(), ",");
    while (tokenizer.hasMoreTokens()) {
      String[] param = tokenizer.nextToken().trim().split("=", 2);
      if (param[1].charAt(0) == '"') {
        param[1] = param[1].substring(1, param[1].length() - 1);
      }
      map.put(param[0], param[1]);
    }

    if (username.equals((String)map.get("username")))
      authenticated = true;
  }

  if (!authenticated) {
    String realm = "realm=\"realm1\"";
    String qop   = "qop=\"auth,auth-int\"";
    String nonce = "nonce=\"dcd98b7102dd2f0e8b11d0f600bfb0c093\"";
    String opaque = "opaque=\"5ccc069c403ebaf9f0171e9517f40e41\"";

    response.setHeader("WWW-Authenticate", "Digest " + realm + ", "
        + qop + ", " + nonce + ", " + opaque);
    response.sendError(response.SC_UNAUTHORIZED);
  } else {
%>
<html>
<head><title>Digest Authentication Test</title></head>
<body>
<p>Hi <%= username %>, you have been successfully authenticated.</p>
</body>
</html>
<%
  }
%>
