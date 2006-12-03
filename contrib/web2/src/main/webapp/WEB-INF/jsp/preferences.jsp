<%--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
--%>
<%@ include file="common.jsp"%>
<tiles:importAttribute />
<form action="preferencesSave.do"><fmt:message
 key="preferences.ui.language" /> <select name="L">
 <c:forEach var="language" items="${languages}">
  <option value="<c:out value="${language}"/>"><fmt:message
   key="${language }" /></option>
 </c:forEach>
</select> <fmt:message key="preferences.ui.language.info" /> <br />
<fmt:message key="preferences.numResults" /> <select name="R">
 <c:forEach var="hits" items="${hitsPerPage}">
  <option value="<c:out value="${hits}"/>"><c:out
   value="${hits}" /></option>
 </c:forEach>
</select> <fmt:message key="preferences.numResults.info" /> <br />
<input type="submit" value="<fmt:message key="preferences.submit"/>" />
</form>
