<%@ include file="common.jsp"%>
<tiles:useAttribute name="title" ignore="true"
	classname="java.lang.String" />
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title><bean:message key="<%=title%>" /></title>
<link rel="icon" href="img/favicon.ico" type="image/x-icon" />
<link rel="shortcut icon" href="img/favicon.ico" type="image/x-icon" />
<link rel="alternate" type="application/rss+xml" title="RSS"
	href="FIXME" />
<tiles:insert name="style" />
<!-- start empty javascript node for popup app fix -->
<script type="text/javascript">
    <!--
function queryfocus() { document.search.query.focus(); }
// -->
    </script>
