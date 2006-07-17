<%@ page session="false"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jstl/fmt" %>
<c:choose>
 <c:when test="${clusteringEnabled != null}">
  <input type="checkbox" name="clustering" value="on" checked="checked"/>
 </c:when>
 <c:otherwise>
  <input id="clustbox" type="checkbox" name="clustering" />
 </c:otherwise>
</c:choose>
 <label for="clustbox"><fmt:message key="search.clustering"/></label>
