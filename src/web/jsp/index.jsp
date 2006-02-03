<%@ page
  session="false"
  import="java.io.*"
  import="java.util.*"
%><%
  String language =
    ResourceBundle.getBundle("org.nutch.jsp.search", request.getLocale())
    .getLocale().getLanguage();
  String requestURI = HttpUtils.getRequestURL(request).toString();
  String base = requestURI.substring(0, requestURI.lastIndexOf('/'));
  response.sendRedirect(language + "/");
%>
