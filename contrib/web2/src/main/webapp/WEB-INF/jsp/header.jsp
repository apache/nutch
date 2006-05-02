<%@ include file="common.jsp"%>
<tiles:useAttribute name="title" ignore="true"
	classname="java.lang.String" />
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title><fmt:message key="${title}" /><c:if test="${nutchSearch!=null}"> - <c:out
		value="${nutchSearch.queryString}" />
</c:if></title>
<link rel="icon" href="img/favicon.ico" type="image/x-icon" />
<link rel="shortcut icon" href="img/favicon.ico" type="image/x-icon" />
<tiles:insert name="style" />
<c:if test="${nutchSearch!=null}">
	<link rel="alternate" type="application/rss+xml" title="RSS"
		href="opensearch?query=<c:out value="${nutchSearch.queryString}"/>" />
	<script type="text/javascript">
    <!--
function queryfocus() { document.search.query.focus(); }
// -->
</script>
</c:if>
