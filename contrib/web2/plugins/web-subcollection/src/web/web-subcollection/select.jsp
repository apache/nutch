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
<%@ page session="false"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jstl/fmt"%>
<div id="subcollection-select"><fmt:message key="subcollection.info" />
<c:forEach var="subcollection" items="${subcollections}">
<c:choose>
<c:when test="${subcollection.checked}">
<input id="<c:out value="${subcollection.id}"/>"
type="radio" name="subcollection" checked="checked"
  value="<c:out value="${subcollection.id}"/>" />
</c:when>
<c:otherwise>
<input id="<c:out value="${subcollection.id}"/>"
type="radio" name="subcollection" 
  value="<c:out value="${subcollection.id}"/>" />
</c:otherwise>
</c:choose>
<label id="<c:out value="${subcollection.id}"/>"><c:out value="${subcollection.name}" /></label>
</c:forEach>
<c:if test="${iscollectionlimited!=null}">
<input id="none"
type="radio" name="subcollection" 
  value="" /><label id="none"><fmt:message key="subcollection.none" /></label>
</c:if>
</div>
