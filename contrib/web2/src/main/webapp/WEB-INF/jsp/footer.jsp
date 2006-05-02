<%@ include file="common.jsp"%>
<div id="footer"><c:if test="${nutchSearch.isSearch=='true'}">
	<table bgcolor="3333ff" align="right">
		<tr>
			<td bgcolor="ff9900"><a
				href="opensearch?query=<c:out value="${nutchSearch.queryString}"/>"><font
				color="ffffff"><b>RSS</b> </font></a></td>
		</tr>
	</table>
</c:if> <a href="http://wiki.apache.org/nutch/FAQ"> <img border="0"
	src="img/poweredbynutch_01.gif"> </a></div>
