<?xml version="1.0" encoding="UTF-8"?>
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
