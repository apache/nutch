<div id="noResults"><%@ include file="common.jsp"%> <jsp:useBean
	id="nutchSearch" scope="request"
	type="org.apache.nutch.webapp.common.Search" /> <bean:message
	key="search.noResults" arg0="<%= nutchSearch.getHtmlQueryString()%>" />
</div>
