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
<c:forEach var="hit" items="${nutchSearch.results}">
 <c:set var="hit" scope="request" value="${hit}" />
 <b><a href="<c:out value="${hit.url}"/>"><c:out
  value="${hit.title}" /></a></b>
 <br />
 <tiles:insert definition="more" ignore="true"/>
 <c:out value="${hit.summary}" escapeXml="false" />
 <br>
 <span class="url"><c:out value="${hit.encodedUrl}" /></span>
    (<a href="cached.do?<c:out value="${hit.id}"/>"><fmt:message
  key="search.cached" /></a>)
    (<a
  href="explain.do?<c:out value="${hit.id}"/>&query=<c:out value="${hit.urlEncodedQuery}"/>"><fmt:message
  key="search.explain" /></a>)
    (<a href="anchors.do?<c:out value="${hit.id}"/>"><fmt:message
  key="search.anchors" /></a>)
<c:if test="${hit.hasMore==true}">
    (<a href="search.do?<c:out value="${hit.moreUrl}"/>"><fmt:message
   key="search.moreFrom" /> <c:out value="${hit.dedupValue}" /></a>)
</c:if>
 <br />
 <br />
</c:forEach>
