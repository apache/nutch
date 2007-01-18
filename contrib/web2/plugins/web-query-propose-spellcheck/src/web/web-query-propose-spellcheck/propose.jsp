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
<%@ taglib prefix="tiles" uri="http://jakarta.apache.org/struts/tags-tiles"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jstl/fmt"%>
<c:if test="${spellCheckerTerms!=null && spellCheckerTerms.hasMispelledTerms}">
 <p>Did you mean <a href="search.do?<c:out value="${spellCheckerQuery}"/>">
 <c:forEach
  var="currentTerm" items="${spellCheckerTerms.terms}">
  <c:out value="${currentItem.charsBefore}" />
  <c:choose>
   <c:when test="${currentTerm.mispelled}">
    <i><b> <c:out value="${currentTerm.suggestedTerm}" /> </b></i>
   </c:when>
   <c:otherwise>
    <c:out value="${currentTerm.originalTerm}" />
   </c:otherwise>
  </c:choose>
  <c:out value="${currentItem.charsAfter}" />
 </c:forEach> </a></p>
</c:if>
