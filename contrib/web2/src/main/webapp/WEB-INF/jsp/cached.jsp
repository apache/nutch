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
<c:if test="${isHtml}">
 <base href="<c:out value="${url}"/>">
</c:if>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<h2 style="{color: rgb(255, 153, 0)}"><fmt:message
 key="cached.title" /></h2>
<h3><fmt:message key="cached.page">
 <fmt:param value="${url}" />
</fmt:message></h3>
<hr />
<c:choose>
 <c:when test="${isHtml}">
  <c:choose>
   <c:when test="${content!=null && content!=''}">
    <c:out value="${content}" escapeXml="false" />
   </c:when>
   <c:otherwise>
    <fmt:message key="cached.noContent" />
   </c:otherwise>
  </c:choose>
 </c:when>
 <c:otherwise>
  <fmt:message key="cached.notHtml">
   <fmt:param value="${contentType}" />
   <fmt:param value="${id}" />
  </fmt:message>
 </c:otherwise>
</c:choose>
