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
<%@ include file="common.jsp"%>
<tiles:useAttribute name="title" ignore="true"
 classname="java.lang.String" />
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title><fmt:message key="${title}" /><c:if
 test="${nutchSearch!=null}"> - <c:out
  value="${nutchSearch.queryString}" />
</c:if></title>
<link rel="icon" href="img/favicon.ico" type="image/x-icon" />
<link rel="shortcut icon" href="img/favicon.ico" type="image/x-icon" />
<tiles:insert name="style" />
<c:if test="${nutchSearch!=null}">
 <link rel="alternate" type="application/rss+xml" title="RSS"
  href="opensearch?query=<c:out value="${nutchSearch.queryString}"/>" />
 <script type="text/javascript">
    <!--
function queryfocus() { document.search.query.focus(); }
// -->
</script>
</c:if>
