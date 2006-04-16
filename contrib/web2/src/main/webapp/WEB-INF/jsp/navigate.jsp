<div id="navigation">
<%@ include file="common.jsp"%>
<logic:equal name="nutchSearch" property="hasNextPage" value="true">
	<form name="search" action="search.do" method="get"><input
		type="submit" value="<bean:message key="search.next"/>"> <logic:iterate
		scope="request" id="urlParameter" name="nutchSearch"
		property="formProperties"
		type="org.apache.nutch.webapp.common.SearchForm.KeyValue">
		<input type="hidden"
			name="<bean:write name="urlParameter" property="key"/>"
			value="<bean:write name="urlParameter" property="value"/>" />
	</logic:iterate></form>
</logic:equal>
</div>