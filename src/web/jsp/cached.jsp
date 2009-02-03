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
  contentType="text/html; charset=UTF-8"
  import="java.io.*"
  import="java.util.*"

  import="org.apache.nutch.searcher.*"
  import="org.apache.nutch.parse.ParseData"
  import="org.apache.nutch.metadata.Metadata"
  import="org.apache.nutch.metadata.Nutch"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.nutch.util.NutchConfiguration"
%><%
  Configuration nutchConf = NutchConfiguration.get(application);
  NutchBean bean = NutchBean.get(application, nutchConf);
  bean.LOG.info("cache request from " + request.getRemoteAddr());
  Hit hit = new Hit(Integer.parseInt(request.getParameter("idx")),
                    request.getParameter("id"));
  HitDetails details = bean.getDetails(hit);
  String id = "idx=" + hit.getIndexNo() + "&id=" + hit.getUniqueKey();

  String language =
    ResourceBundle.getBundle("org.nutch.jsp.cached", request.getLocale())
    .getLocale().getLanguage();

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
      }
      catch (UnsupportedEncodingException e) {
        // fallback to windows-1252
        content = new String(bean.getContent(details), "windows-1252");
      }
    }
    else 
      content = new String(bean.getContent(details));
  }
%>
<!--
<base href="<%=details.getValue("url")%>">
-->
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<%
  out.flush();
%>
<%@ taglib uri="http://jakarta.apache.org/taglibs/i18n" prefix="i18n" %>
<i18n:bundle baseName="org.nutch.jsp.cached"/>
<h2 style="{color: rgb(255, 153, 0)}"><i18n:message key="title"/></h2>
<h3>
<i18n:message key="page">
  <i18n:messageArg value="<%=details.getValue(\"url\")%>"/>
</i18n:message>
</h3>
<hr>
<!-- 
   FIXME: have to sanitize 'content' : e.g. removing unncessary part
        of head elememt
-->
<%
   String caching = details.getValue("cache");
   String url = details.getValue("url");
   if (caching != null && !caching.equals(Nutch.CACHING_FORBIDDEN_NONE)) {
%>
Display of this content was administratively prohibited by the webmaster.
You may visit the original page instead: <a href="<%=url%>"><%=url%></a>.
<%
     return;
   }
%>
<% if (contentType.startsWith("text/html")) {%>

<% if (content != null && !content.equals("")) {%>
<%= content %>
<% } else { %>
<i18n:message key="noContent"/>
<% } %>

<% } else { %>

The cached content has mime type "<%=contentType%>",
click this <a href="./servlet/cached?<%=id%>">link</a> to download it directly.

<% } %>
