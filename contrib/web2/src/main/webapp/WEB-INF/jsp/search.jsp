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
<c:choose>
 <c:when test="${nutchSearch.isSearch}">
  <form name="search" action="search.do" method="get"><input
   name="query" size="44"
   value="<c:out value="${nutchSearch.queryString}"/>"> <input
   type="submit" value="<fmt:message key="search.search"/>"> <a
   href="help.do"><fmt:message key="search.help" /></a></form>
  <c:choose>
   <c:when test="${nutchSearch.hasResults}">
    <fmt:message key="search.hits">
     <fmt:param value="${nutchSearch.resultInfo[0]}" />
     <fmt:param value="${nutchSearch.resultInfo[1]}" />
     <fmt:param value="${nutchSearch.resultInfo[2]}" />
     <fmt:param value="${nutchSearch.resultInfo[3]}" />
    </fmt:message>
    <br />
    <tiles:insert name="results" flush="true" />
    <!-- optional tile  -->
    <tiles:insert definition="cluster" ignore="true" />
    <tiles:insert name="navigate" />
   </c:when>
   <c:otherwise>
    <tiles:insert name="noResults" />
   </c:otherwise>
  </c:choose>
 </c:when>
 <c:otherwise>
  <tiles:insert name="i18nComponent">
   <tiles:put name="basePage" value="/search.html" />
  </tiles:insert>
 </c:otherwise>
</c:choose>
