<%@ page session="false"%>
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
<%@ taglib prefix="tiles" uri="http://jakarta.apache.org/struts/tags-tiles" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jstl/fmt" %>
<%
// @author Dawid Weiss
//
// PERFORMANCE/USER INTERFACE NOTE:
//
// What I do here is merely a demonstration. In real life the clustering
// process should be done in a separate "processing" stream, most likely
// a separate HTML frame that the user's browser requests data to.
// We don't want the user to wait with plain snippets until the clusters
// are created.
//
// Also: clustering is resource consuming, so a cache of recent queries 
// would be in place. Besides, such cache would also be beneficial for the
// purpose of re-querying existing clusters (remember that the
// clustering extension may be a heuristic returning a DIFFERENT set of
// clusters for an identical input).
// See www.vivisimo.com for details of how this can be done using frames, or
// http://carrot.cs.put.poznan.pl for an example of a Javascript solution.
%>
<div id="cluster"><c:choose>
 <c:when test="${clusters!=null}">

  <c:choose>
   <c:when test="${clusters.hasClusters}">
    <c:forEach var="cluster" items="${clusters.clusters}">
     <div style="margin: 0px; padding: 0px; font-weight: bold;"><c:out
      value="${cluster.label}"></c:out><br />
     </div>
     <c:forEach var="doc" items="${cluster.docs}">
      <li><a href="<c:out value="${doc.url}"/>"><c:out
       value="${doc.title}" /></a></li>
     </c:forEach>
    </c:forEach>
   </c:when>
   <c:otherwise>
    <!--  todo: i18n -->
No clusters available
</c:otherwise>
  </c:choose>

 </c:when>
</c:choose>
</div>
