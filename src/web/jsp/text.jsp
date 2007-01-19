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
<%@ page
  session="false"

  import="java.io.*"
  import="java.util.*"

  import="org.apache.nutch.searcher.*"
  import="org.apache.nutch.parse.ParseText"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.nutch.util.NutchConfiguration"

%><%

  // show the content of a hit as plain text
  Configuration nutchConf = NutchConfiguration.get(application);
  NutchBean bean = NutchBean.get(application, nutchConf);

  bean.LOG.info("text request from " + request.getRemoteAddr());

  Hit hit = new Hit(Integer.parseInt(request.getParameter("idx")),
                    Integer.parseInt(request.getParameter("id")));
  HitDetails details = bean.getDetails(hit);

  String text = bean.getParseText(details).getText();
  if (text.trim().equals(""))
    text = null;

  // 20041005, xing
  // This "CharEncodingForConversion" thing is only pertinent to
  // html parser (plugin parse-html) in current nutch. None of
  // other parser plugins are into it. So we worry it later.

%><base href="<%=details.getValue("url")%>">
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<%
  out.flush();
%>

<%@ taglib uri="http://jakarta.apache.org/taglibs/i18n" prefix="i18n" %>
<i18n:bundle baseName="org.nutch.jsp.text"/>
<h2 style="{color: rgb(255, 153, 0)}"><i18n:message key="title"/></h2>

<i18n:message key="note">
  <i18n:messageArg value="<%=details.getValue("url")%>"/>
</i18n:message>

<hr>

<% if (text != null) {%>
<pre>
<%= text %>
</pre>
<% } else { %>
<i18n:message key="noText"/>
<% } %>
