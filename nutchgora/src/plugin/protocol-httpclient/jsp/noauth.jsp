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
  This JSP tests whether the client is sending any pre-emptive
  authentication headers. The client is expected not to send pre-emptive
  authentication headers. If such authentication headers are found, this
  JSP will return an HTTP 403 response; HTTP 200 response otherwise.

  Author: Susam Pal
--%>
<%
  if (request.getHeader("Authorization") != null) {
    response.sendError(response.SC_UNAUTHORIZED);
  } else {
%>
<html>
<head><title>No authorization headers found</title></head>
<body>
<p>No authorization headers found.</p>
</body>
</html>
<%
  }
%>
