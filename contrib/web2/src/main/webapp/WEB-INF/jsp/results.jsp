<%@ include file="common.jsp" %>
<c:forEach var="hit" items="${nutchSearch.results}">
  <c:set var="hit" scope="request" value="${hit}"/>
    <b><a href="<c:out value="${hit.url}"/>"><c:out value="${hit.title}"/></a></b><br/>    
    <tiles:insert name="more" />
    <c:out value="${hit.summary}" escapeXml="false"/>
    <br>
    <span class="url"><c:out value="${hit.encodedUrl}"/></span>
    (<a href="cached.do?<c:out value="${hit.id}"/>"><fmt:message key="search.cached"/></a>)
    (<a href="explain.do?<c:out value="${hit.id}"/>&query=<c:out value="${hit.urlEncodedQuery}"/>"><fmt:message key="search.explain"/></a>)
    (<a href="anchors.do?<c:out value="${hit.id}"/>"><fmt:message key="search.anchors"/></a>)
<c:if test="${hit.hasMore==true}">
    (<a href="search.do?<c:out value="${hit.moreUrl}"/>"><fmt:message key="search.moreFrom"/>
    <c:out value="${hit.dedupValue}"/></a>)
</c:if>
<br/><br/>
</c:forEach>
