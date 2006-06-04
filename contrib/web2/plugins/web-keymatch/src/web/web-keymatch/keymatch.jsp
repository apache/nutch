<%@ page session="false"%>
<%@ taglib prefix="tiles" uri="http://jakarta.apache.org/struts/tags-tiles"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jstl/fmt"%>
<div id="keymatch">
<c:forEach var="keymatch" items="${keymatches}">
 <a href="search.do?query=<c:out value="${keymatch.url}"/>">
 <c:out value="${keymatch.title}"/>
 </a><br/>
</c:forEach>
</div>
