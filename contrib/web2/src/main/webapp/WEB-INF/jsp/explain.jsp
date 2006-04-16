<%@ include file="common.jsp" %>
<h3><bean:message key="explain.page"/></h3>
<bean:parameter id="query" name="query"/>
<bean:write name="hitDetails" filter="false"/>
<h3><bean:message arg0="<%=query%>" key="explain.scoreForQuery" />
</h3>
<bean:write name="explanation" filter="false"/> 
