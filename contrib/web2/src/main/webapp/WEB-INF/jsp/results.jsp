<%@ include file="common.jsp" %>
<logic:iterate scope="request" id="hit" name="nutchSearch" property="results" type="org.apache.nutch.webapp.common.SearchResultBean">
    <b><a href="<bean:write name="hit" property="url"/>"><bean:write name="hit" property="title"/></a></b><br/>    
    <tiles:insert name="more" flush="false" beanName="hit"/>
    <bean:write name="hit" property="summary" filter="false"/>
    <br>
    <span class="url"><bean:write name="hit" property="encodedUrl"/></span>
    (<a href="cached.do?<bean:write name="hit" property="id"/>"><bean:message key="search.cached"/></a>)
    (<a href="explain.do?<bean:write name="hit" property="id"/>&query=<bean:write name="hit" property="urlEncodedQuery"/>"><bean:message key="search.explain"/></a>)
    (<a href="anchors.do?<bean:write name="hit" property="id"/>"><bean:message key="search.anchors"/></a>)
<logic:equal name="hit" property="hasMore" value="true">
    (<a href="search.do?<bean:write name="hit" property="moreUrl"/>"><bean:message key="search.moreFrom"/>
    <bean:write name="hit" property="dedupValue"/></a>)
</logic:equal>
<br/><br/>
</logic:iterate>
</div>
