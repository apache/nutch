<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"

  import="org.apache.nutch.searcher.*"
  import="org.apache.nutch.parse.ParseData"
  import="org.apache.nutch.protocol.ContentProperties"
%><%
  NutchBean bean = NutchBean.get(application);
  bean.LOG.info("cache request from " + request.getRemoteAddr());
  Hit hit = new Hit(Integer.parseInt(request.getParameter("idx")),
                    Integer.parseInt(request.getParameter("id")));
  HitDetails details = bean.getDetails(hit);
  String id = "idx=" + hit.getIndexNo() + "&id=" + hit.getIndexDocNo();

  String language =
    ResourceBundle.getBundle("org.nutch.jsp.cached", request.getLocale())
    .getLocale().getLanguage();

  ContentProperties metaData = bean.getParseData(details).getMetadata();

  String content = null;
  String contentType = (String) metaData.get("Content-Type");
  if (contentType.startsWith("text/html")) {
    // FIXME : it's better to emit the original 'byte' sequence 
    // with 'charset' set to the value of 'CharEncoding',
    // but I don't know how to emit 'byte sequence' in JSP.
    // out.getOutputStream().write(bean.getContent(details)) may work, 
    // but I'm not sure.
    String encoding = (String) metaData.get("CharEncodingForConversion"); 
    if (encoding != null) {
      try {
        content = new String(bean.getContent(details), encoding);
      }
      catch (UnsupportedEncodingException e) {
        // fallback to windows-1252
        content = new String(bean.getContent(details), "windows-1252");
      }
    }
    else 
      content = new String(bean.getContent(details));
  }
%>
<!--
<base href="<%=details.getValue("url")%>">
-->
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<%
  out.flush();
%>
<%@ taglib uri="http://jakarta.apache.org/taglibs/i18n" prefix="i18n" %>
<i18n:bundle baseName="org.nutch.jsp.cached"/>
<h2 style="{color: rgb(255, 153, 0)}"><i18n:message key="title"/></h2>
<h3>
<i18n:message key="page">
  <i18n:messageArg value="<%=details.getValue("url")%>"/>
</i18n:message>
</h3>
<hr>
<!-- 
   FIXME: have to sanitize 'content' : e.g. removing unncessary part
        of head elememt
-->
<% if (contentType.startsWith("text/html")) {%>

<% if (content != null && !content.equals("")) {%>
<%= content %>
<% } else { %>
<i18n:message key="noContent"/>
<% } %>

<% } else { %>

The cached content has mime type "<%=contentType%>",
click this <a href="./servlet/cached?<%=id%>">link</a> to download it directly.

<% } %>
