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
