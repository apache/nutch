<%@ include file="common.jsp"%>
<h3><fmt:message key="anchors.page">
	<fmt:param value="${nutchAnchors.url}" />
</fmt:message></h3>
<h3><fmt:message key="anchors.anchors" /></h3>
<ul>
	<c:forEach var="anchor" items="${nutchAnchors.anchors}">
		<li><c:out value="${anchor}" /></li>
	</c:forEach>
</ul>
<br />
