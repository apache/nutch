<%@ include file="common.jsp" %>
<jsp:useBean id="hit" scope="request" type="org.apache.nutch.webapp.SearchResultBean"/>
<%
    // @author John Xing
    // show meta info (currently type, size, date of last-modified)
    // for each hit. These info are indexed by ./src/plugin/index-more.

    // do not show unless we have something
    boolean showMore = false;

    // Content-Type
    String primaryType = hit.getDetails().getValue("primaryType");
    String subType = hit.getDetails().getValue("subType");

    String contentType = subType;
    if (contentType == null)
      contentType = primaryType;
    if (contentType != null) {
      contentType = "[<span class=\"contentType\">" + contentType + "</span>]";
      showMore = true;
    } else {
      contentType = "";
    }

    // Content-Length
    String contentLength = hit.getDetails().getValue("contentLength");
    if (contentLength != null) {
      contentLength = "(" + contentLength + " bytes)";
      showMore = true;
    } else {
      contentLength = "";
    }

    // Last-Modified
    String lastModified = hit.getDetails().getValue("lastModified");
    if (lastModified != null) {
      java.util.Calendar cal = new java.util.GregorianCalendar();
      cal.setTimeInMillis(new Long(lastModified).longValue());
      lastModified = cal.get(java.util.Calendar.YEAR)
                  + "." + (1+cal.get(java.util.Calendar.MONTH)) // it is 0-based
                  + "." + cal.get(java.util.Calendar.DAY_OF_MONTH);
      showMore = true;
    } else {
      lastModified = "";
    }
%>

<% if (showMore) {
    if ("text".equalsIgnoreCase(primaryType)) { %>
    <br><font size=-1><nobr><%=contentType%> <%=contentLength%> <%=lastModified%></nobr></font>
<%  } else { %>
    <br><font size=-1><nobr><%=contentType%> <%=contentLength%> <%=lastModified%> - <a href="../text.jsp?<jsp:getProperty name="hit" property="id"/>"><i18n:message key="viewAsText"/></a></nobr></font>
<%  }
  } %>
