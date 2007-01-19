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
  pageEncoding="UTF-8"

  import="java.io.*"
  import="java.util.*"

  import="org.apache.nutch.html.Entities"
  import="org.apache.nutch.searcher.*"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.nutch.util.NutchConfiguration"
%><%
  Configuration nutchConf = NutchConfiguration.get(application);
  NutchBean bean = NutchBean.get(application, nutchConf);
  // set the character encoding to use when interpreting request values 
  request.setCharacterEncoding("UTF-8");
  bean.LOG.info("anchors request from " + request.getRemoteAddr());
  Hit hit = new Hit(Integer.parseInt(request.getParameter("idx")),
                    Integer.parseInt(request.getParameter("id")));
  HitDetails details = bean.getDetails(hit);
  String language =
    ResourceBundle.getBundle("org.nutch.jsp.anchors", request.getLocale())
    .getLocale().getLanguage();
  String requestURI = HttpUtils.getRequestURL(request).toString();
  String base = requestURI.substring(0, requestURI.lastIndexOf('/'));
%><!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<%
  // To prevent the character encoding declared with 'contentType' page
  // directive from being overriden by JSTL (apache i18n), we freeze it
  // by flushing the output buffer. 
  // see http://java.sun.com/developer/technicalArticles/Intl/MultilingualJSP/
  out.flush();
%>
<%@ taglib uri="http://jakarta.apache.org/taglibs/i18n" prefix="i18n" %>
<i18n:bundle baseName="org.nutch.jsp.anchors"/>
<html lang="<%= language %>">
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<head>
<title>Nutch: <i18n:message key="title"/></title>
<jsp:include page="/include/style.html"/>
<base href="<%= base + "/" + language + "/" %>">
</head>

<body>

<jsp:include page="<%= language + "/include/header.html"%>"/>

<h3>
<i18n:message key="page">
  <i18n:messageArg value="<%=details.getValue("url")%>"/>
</i18n:message>
</h3>

<h3><i18n:message key="anchors"/></h3>

<ul>
<%
  String[] anchors = bean.getAnchors(details);
  if (anchors != null) {
    for (int i = 0; i < anchors.length; i++) {
%><li><%=Entities.encode(anchors[i])%>
<%   } %>
<% } %>
</ul>
     
<jsp:include page="/include/footer.html"/>

</body>     
</html>
