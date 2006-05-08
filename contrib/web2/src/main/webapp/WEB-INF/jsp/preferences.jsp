<%@ include file="common.jsp"%>
<tiles:importAttribute/>
<form action="preferencesSave.do"><fmt:message
	key="preferences.ui.language" /> <select name="L">
	<c:forEach var="language" items="${languages}">
		<option value="<c:out value="${language}"/>"><fmt:message
			key="${language }" /></option>
	</c:forEach>
</select> <fmt:message key="preferences.ui.language.info" /> <br />
<fmt:message key="preferences.numResults" /> <select name="R">
	<c:forEach var="hits" items="${hitsPerPage}">
		<option value="<c:out value="${hits}"/>"><c:out value="${hits}" /></option>
	</c:forEach>
</select> <fmt:message key="preferences.numResults.info" /> <br />
<input type="submit" value="<fmt:message key="preferences.submit"/>" />
</form>
