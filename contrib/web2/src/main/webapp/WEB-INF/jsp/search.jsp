<%@ include file="common.jsp"%>
<logic:match name="nutchSearch" property="isSearch" value="false">
	<jsp:useBean id="resultInfo" scope="request" type="String[]" />
	<form name="search" action="search.do" method="get"><input name="query"
		size=44
		value="<bean:write name="nutchSearch" property="queryString"/>"> <input
		type="hidden" name="hitsPerPage"
		value="<bean:write name="nutchSearch" property="hitsPerPage"/>"> <input
		type="submit" value="<bean:message key="search.search" />"> <a
		href="help.do"><bean:message key="search.help" /></a></form>
	<logic:match name="nutchSearch" property="hasResults" value="true">
		<bean:message key="search.hits" arg0="<%=resultInfo[0]%>"
			arg1="<%=resultInfo[1]%>" arg2="<%=resultInfo[2]%>"
			arg3="<%=resultInfo[3]%>" />
		<br />
		<tiles:insert name="results" />
		<tiles:insert name="cluster" />
		<tiles:insert name="navigate" />
	</logic:match>
	<logic:notMatch name="nutchSearch" property="hasResults" value="true">
		<tiles:insert name="noResults" />
	</logic:notMatch>
</logic:match>
<logic:notMatch name="nutchSearch" property="isSearch" value="false">
	<tiles:insert name="i18nComponent">
		<tiles:put name="basePage" value="/search.html" />
	</tiles:insert>
</logic:notMatch>
