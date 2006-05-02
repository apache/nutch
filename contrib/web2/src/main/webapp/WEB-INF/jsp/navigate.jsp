<%@ include file="common.jsp"%>
<div id="navigation">
<c:if test="${nutchSearch.hasNextPage==true}">
	<form name="search" action="search.do" method="get"><input
		type="submit" value="<fmt:message key="search.next"/>">
		<c:forEach var="urlParameter" items="${nutchSearch.formProperties}">
		<input type="hidden"
			name="<c:out value="${urlParameter.key}"/>"
			value="<c:out value="${urlParameter.value}"/>" />
	</c:forEach></form>
</c:if>
<c:if test="${nutchSearch.showAllHits==true}">
	<form name="search" action="search.do" method="get"><input
		type="submit" value="<fmt:message key="search.showAllHits"/>">
		<c:forEach var="urlParameter" items="${nutchSearch.formProperties}">
		<input type="hidden"
			name="<c:out value="${urlParameter.key}"/>"
			value="<c:out value="${urlParameter.value}"/>" />
	</c:forEach></form>
</c:if>
</div>