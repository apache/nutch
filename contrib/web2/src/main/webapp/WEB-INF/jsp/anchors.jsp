<%@ include file="common.jsp"%>
<jsp:useBean id="nutchAnchors" scope="request"
	type="org.apache.nutch.webapp.controller.AnchorsController.AnchorsBean" />
<h3><bean:message key="anchors.page" arg0="<%=nutchAnchors.getUrl()%>" />
</h3>
<h3><bean:message key="anchors.anchors" /></h3>
<ul>
	<logic:iterate scope="request" id="anchor" name="nutchAnchors"
		property="anchors" type="String">
		<li><bean:write name="anchor" /></li>
	</logic:iterate>
</ul>
<br />
