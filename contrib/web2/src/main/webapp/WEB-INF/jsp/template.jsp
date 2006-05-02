<%@page contentType="text/html; charset=utf-8" session="false"%>
<%@ include file="common.jsp"%>
<html lang="<fmt:message key="lang"/>">
<head>
<tiles:insert name="header">
	<tiles:put name="title" beanName="title" />
	<tiles:put name="basePage" content="/include/header.html" />
	<tiles:put name="attrName" content="headerContent" />
</tiles:insert>
</head>
<body onLoad="queryfocus();">
<c:out default="" value="${headerContent}" escapeXml="false"/>
<tiles:insert name="pageBody" />
<tiles:insert name="footer" />
</body>
</html>

