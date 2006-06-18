<%@ page session="false"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jstl/fmt"%>
<div id="subcollection-select"><fmt:message key="subcollection.info" />
<c:forEach var="subcollection" items="${subcollections}">
<c:choose>
<c:when test="${subcollection.checked}">
<input id="<c:out value="${subcollection.id}"/>"
type="radio" name="subcollection" checked="checked"
  value="<c:out value="${subcollection.id}"/>" />
</c:when>
<c:otherwise>
<input id="<c:out value="${subcollection.id}"/>"
type="radio" name="subcollection" 
  value="<c:out value="${subcollection.id}"/>" />
</c:otherwise>
</c:choose>
<label id="<c:out value="${subcollection.id}"/>"><c:out value="${subcollection.name}" /></label>
</c:forEach>
<c:if test="${iscollectionlimited!=null}">
<input id="none"
type="radio" name="subcollection" 
  value="" /><label id="none"><fmt:message key="subcollection.none" /></label>
</c:if>
</div>
