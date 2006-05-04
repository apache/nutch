<%@ include file="common.jsp"%>
<c:choose>
	<c:when test="${nutchSearch.isSearch == true}">
		<form name="search" action="search.do" method="get"><input
			name="query" size="44"
			value="<c:out value="${nutchSearch.queryString}"/>"> <input
			type="submit" value="<fmt:message key="search.search"/>"> <a
			href="help.do"><fmt:message key="search.help" /></a></form>
		<c:choose>
			<c:when test="${nutchSearch.hasResults == true }">
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
			</c:when>
			<c:otherwise>
				<tiles:insert name="noResults" />
			</c:otherwise>
		</c:choose>
	</c:when>
	<c:otherwise>
		<tiles:insert name="i18nComponent">
			<tiles:put name="basePage" value="/search.html" />
		</tiles:insert>
	</c:otherwise>
</c:choose>
