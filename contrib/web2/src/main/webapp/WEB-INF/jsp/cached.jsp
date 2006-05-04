<%@ include file="common.jsp"%>
<c:if test="${isHtml}">
  <base href="<c:out value="${url}"/>">
</c:if>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<h2 style="{color: rgb(255, 153, 0)}"><fmt:message key="cached.title" /></h2>
<h3><fmt:message key="cached.page">
	<fmt:param value="${url}" />
</fmt:message></h3>
<hr />
<c:choose>
	<c:when test="${isHtml}">
		<c:choose>
			<c:when test="${content!=null && content!=''}">
				<c:out value="${content}" escapeXml="false"/>
			</c:when>
			<c:otherwise>
				<fmt:message key="cached.noContent" />
			</c:otherwise>
		</c:choose>
	</c:when>
	<c:otherwise>
		<fmt:message key="cached.notHtml">
			<fmt:param value="${contentType}" />
			<fmt:param value="${id}" />
		</fmt:message>
	</c:otherwise>
</c:choose>
