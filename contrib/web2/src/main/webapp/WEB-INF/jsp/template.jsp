<%@page contentType="text/html; charset=utf-8" session="false"%>
<%@ include file="common.jsp"%>
<html lang="<fmt:message key="lang"/>">
<head>
<tiles:insert name="header" flush="true">
	<tiles:put name="title" beanName="title" />
</tiles:insert>
</head>
<body onLoad="queryfocus();">
<c:out default="" value="${headerContent}" escapeXml="false"/>
<tiles:insert name="pageBody" flush="true"/>
<tiles:insert name="footer" />
</body>
</html>

