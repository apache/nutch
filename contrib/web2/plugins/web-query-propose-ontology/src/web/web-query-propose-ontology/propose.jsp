<%@ page session="false"%>
<%@ taglib prefix="tiles" uri="http://jakarta.apache.org/struts/tags-tiles"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jstl/fmt"%>
<%
// 20041129, Mike Pan and John Xing
// Displays query-refinement hypertext based on ontology.
// Try to be simple here. No mixing with other features such as clustering.
// Please check refine-query-init.jsp, which does necessary initialization.
%>
<div id="refine">Refine your search:
<c:forEach var="propose" items="${queryPropose}">
 <a href="search.do?query=<c:out value="${propose}"/>">
 <c:out value="${propose}"/>
 </a> |
</c:forEach>
