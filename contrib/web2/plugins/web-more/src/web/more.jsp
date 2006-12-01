<?xml version="1.0" encoding="UTF-8"?>
<!--
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
-->
<jsp:root xmlns:jsp="http://java.sun.com/JSP/Page" version="1.2"
 xmlns:c="/tags/jstl-c" xmlns:tiles="/tags/struts-tiles"
 xmlns:fmt="/tags/jstl-fmt">
 <jsp:directive.page session="false" />
 <c:if
  test="${contentType!=null || contentLength!=null ||lastModified!=null }">
  <c:if test="${contentLength!=null}">
   <fmt:message key="search.contentType">
    <fmt:param value="${contentType}" />
   </fmt:message>
  </c:if>
  <c:if test="${contentLength!=null}">
   <fmt:message key="search.contentLength">
    <fmt:param value="${contentLength}" />
   </fmt:message>
  </c:if>
  <c:if test="${contentLength!=null}">
   <fmt:message key="search.lastModified">
    <fmt:param>
     <fmt:formatDate value="${lastModified}" />
    </fmt:param>
   </fmt:message>
  </c:if>
  <br />
 </c:if>
</jsp:root>
