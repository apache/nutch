<%@ include file="common.jsp"%>
<tiles:useAttribute name="title" ignore="true"
	classname="java.lang.String" />
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title><bean:message name="title"/></title>
<link rel="icon" href="img/favicon.ico" type="image/x-icon" />
<link rel="shortcut icon" href="img/favicon.ico" type="image/x-icon" />
<tiles:insert name="style" />
<logic:present name="nutchSearch">
<link rel="alternate" type="application/rss+xml" title="RSS"
	href="opensearch?query=<bean:write name="nutchSearch" property="queryString"/>"/>
<script type="text/javascript">
    <!--
function queryfocus() { document.search.query.focus(); }
// -->
</script>
</logic:present>