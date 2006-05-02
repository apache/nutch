<%@ include file="common.jsp" %>
<h3><fmt:message key="explain.page"/></h3>
<c:out value="${hitDetails}" escapeXml="false"/>
<h3><fmt:message key="explain.scoreForQuery"><fmt:param value="${query}"/></fmt:message>
</h3>
<c:out value="${explanation}" escapeXml="false"/>
