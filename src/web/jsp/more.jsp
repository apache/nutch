<%
    // @author John Xing
    // show meta info (currently type, size, date of last-modified)
    // for each hit. These info are indexed by ./src/plugin/index-more.

    // do not show unless we have something
    boolean showMore = false;

    // Content-Type
    String primaryType = detail.getValue("primaryType");
    String subType = detail.getValue("subType");

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
    String contentLength = detail.getValue("contentLength");
    if (contentLength != null) {
      contentLength = "(" + contentLength + " bytes)";
      showMore = true;
    } else {
      contentLength = "";
    }

    // Last-Modified
    String lastModified = detail.getValue("lastModified");
    if (lastModified != null) {
      Calendar cal = new GregorianCalendar();
      cal.setTimeInMillis(new Long(lastModified).longValue());
      lastModified = cal.get(Calendar.YEAR)
                  + "." + (1+cal.get(Calendar.MONTH)) // it is 0-based
                  + "." + cal.get(Calendar.DAY_OF_MONTH);
      showMore = true;
    } else {
      lastModified = "";
    }
%>

<% if (showMore) {
    if ("text".equalsIgnoreCase(primaryType)) { %>
    <br><font size=-1><nobr><%=contentType%> <%=contentLength%> <%=lastModified%></nobr></font>
<%  } else { %>
    <br><font size=-1><nobr><%=contentType%> <%=contentLength%> <%=lastModified%> - <a href="../text.jsp?<%=id%>"><i18n:message key="viewAsText"/></a></nobr></font>
<%  }
  } %>
