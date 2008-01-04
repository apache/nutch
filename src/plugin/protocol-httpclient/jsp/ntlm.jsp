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
  This JSP tests NTLM authentication. It generates an HTTP response
  with authorization header for NTLM authentication and checks the
  user-name supplied by the client. It does not check the other
  parameters and hashes as controlled JUnit tests would be performed
  against this and only the proper submission of credentials need to
  be tested.

  Author: Susam Pal
--%>
<%@ page
    import = "sun.misc.BASE64Decoder"
    import = "sun.misc.BASE64Encoder"
%>
<%
  String authHeader = request.getHeader("Authorization");
  String username = null;
  String domain = null;
  String host = null;

  boolean authenticated = false;
  if (authHeader != null && authHeader.startsWith("NTLM")) {
    byte[] msg = new BASE64Decoder().decodeBuffer(
        authHeader.substring(5));
    if (msg[8] == 1) {
      byte[] type2msg = {
          'N', 'T', 'L', 'M', 'S', 'S', 'P', 0, // NTLMSSP Signature
          2, 0, 0, 0,                           // Type 2 Indicator
          10, 0, 10, 0, 32, 0, 0, 0,            // length, offset
          0x00, 0x02, (byte) 0x81, 0,           // Flags
          1, 2, 3, 4, 5, 6, 7, 8,               // Challenge
          'N', 'U', 'T', 'C', 'H' // NUTCH (Domain)
      };
      response.setHeader("WWW-Authenticate", "NTLM "
          + new BASE64Encoder().encodeBuffer(type2msg));
      response.sendError(response.SC_UNAUTHORIZED);
      return;
    } else if (msg[8] == 3) {
      int length;
      int offset;

      // Get domain name
      length = msg[30] + msg[31] * 256;
      offset = msg[32] + msg[33] * 256;
      domain = new String(msg, offset, length);

      // Get user name
      length = msg[38] + msg[39] * 256;
      offset = msg[40] + msg[41] * 256;
      username = new String(msg, offset, length);

      // Get password
      length = msg[46] + msg[47] * 256;
      offset = msg[48] + msg[49] * 256;
      host = new String(msg, offset, length);

      if ("ntlm_user".equalsIgnoreCase(username)
          && "NUTCH".equalsIgnoreCase(domain))
        authenticated = true;
    }
  }

  if (!authenticated) {
    response.setHeader("WWW-Authenticate", "NTLM");
    response.sendError(response.SC_UNAUTHORIZED);
  } else {
%>
<html>
<head>NTLM Authentication Test</head>
<body>
<p>Hi <%= username %>, You have been successfully authenticated.</p>
</body>
</html>
<%
  }
%>
