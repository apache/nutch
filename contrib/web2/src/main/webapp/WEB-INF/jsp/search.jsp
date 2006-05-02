<%@ include file="common.jsp"%>
<c:if test="${nutchSearch.isSearch == true}">
	<form name="search" action="search.do" method="get"><input name="query"
		size="44" value="<c:out value="${nutchSearch.queryString}"/>">
		<input
		type="submit" value="<fmt:message key="search.search"/>"> <a
		href="help.do"><fmt:message key="search.help" /></a></form>
	<c:if test="${nutchSearch.hasResults == true }">
		<fmt:message key="search.hits">
			<fmt:param value="${resultInfo[0]}" />
			<fmt:param value="${resultInfo[1]}" />
			<fmt:param value="${resultInfo[2]}" />
			<fmt:param value="${resultInfo[3]}" />
		</fmt:message>
		<br />
		<tiles:insert name="results" />
		<tiles:insert name="cluster" />
		<tiles:insert name="navigate" />
	</c:if>
	<c:if test="${nutchSearch.hasResults == false }">
		<tiles:insert name="noResults" />
	</c:if>
</c:if>
<c:if test="${nutchSearch.isSearch == false}">
	<tiles:insert name="i18nComponent">
		<tiles:put name="basePage" value="/search.html" />
	</tiles:insert>
</c:if>