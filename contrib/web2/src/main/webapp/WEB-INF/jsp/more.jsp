<%@ include file="common.jsp"%>
<c:if
	test="${contentType!=null || contentLength!=null ||lastModified!=null }">
	<fmt:message key="search.contentType">
		<fmt:param value="${contentType}" />
	</fmt:message>
	<fmt:message key="search.contentLength">
		<fmt:param value="${contentLength}" />
	</fmt:message>
	<fmt:message key="search.lastModified">
	<fmt:param><fmt:formatDate value="${lastModified}" /></fmt:param>
	</fmt:message>
	<br />
</c:if>
