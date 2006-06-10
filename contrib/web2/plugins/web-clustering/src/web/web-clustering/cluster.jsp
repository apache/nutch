<%@ page session="false"%>
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
