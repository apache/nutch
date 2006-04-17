<%@ include file="common.jsp"%>
<div id="footer"><logic:present name="nutchSearch">
	<table bgcolor="3333ff" align="right">
		<tr>
			<td bgcolor="ff9900"><a href="opensearch?query=<bean:write name="nutchSearch" property="queryString"/>"><font color="ffffff"><b>RSS</b>
			</font></a></td>
		</tr>
	</table>
</logic:present> <a href="http://wiki.apache.org/nutch/FAQ"> <img
	border="0" src="img/poweredbynutch_01.gif"> </a></div>
