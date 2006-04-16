<%@page contentType="text/html; charset=utf-8" session="false"%>
<%@ include file="common.jsp"%>
<html lang="<bean:message key="lang"/>">
<head>
<tiles:insert name="header">
	<tiles:put name="title" beanName="title" />
	<tiles:put name="basePage" content="/include/header.html" />
	<tiles:put name="attrName" content="header" />
</tiles:insert>
</head>
   <body onLoad="queryfocus();">
   <bean:write name="header" scope="request" filter="false" ignore="true"/>
<tiles:insert name="pageBody" />
<tiles:insert name="footer" />
 </body>
</html>

