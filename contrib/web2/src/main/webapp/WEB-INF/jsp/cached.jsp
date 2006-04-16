<%@ page
  session="false"
  contentType="text/html; charset=UTF-8"
  import="java.io.*"
  import="java.util.*"

  import="org.apache.nutch.searcher.*"
  import="org.apache.nutch.parse.ParseData"
  import="org.apache.nutch.metadata.Metadata"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.nutch.util.NutchConfiguration"
%><%@ taglib prefix="bean" uri="/tags/struts-bean" %><%
  Configuration nutchConf = (Configuration) application.getAttribute(Configuration.class.getName());
  if (nutchConf == null) {
    nutchConf = NutchConfiguration.create();
    application.setAttribute(Configuration.class.getName(), nutchConf);
  }
  NutchBean bean = NutchBean.get(application, nutchConf);
  bean.LOG.info("cache request from " + request.getRemoteAddr());
  Hit hit = new Hit(Integer.parseInt(request.getParameter("idx")),
                    Integer.parseInt(request.getParameter("id")));
  HitDetails details = bean.getDetails(hit);
  String id = "idx=" + hit.getIndexNo() + "&id=" + hit.getIndexDocNo();

  Metadata metaData = bean.getParseData(details).getContentMeta();

  String content = null;
  String contentType = (String) metaData.get(Metadata.CONTENT_TYPE);
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
<base href="<%=details.getValue("url")%>">
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<h2 style="{color: rgb(255, 153, 0)}"><bean:message key="cached.title"/></h2>
<h3><bean:message key="cached.page" arg0="<%=details.getValue("url")%>" /></h3>
<hr>
<% if (contentType.startsWith("text/html")) {%>

<% if (content != null && !content.equals("")) {%>
<%= content %>
<% } else { %>
<bean:message key="cached.noContent"/>
<% } %>

<% } else { %>

The cached content has mime type "<%=contentType%>",
click this <a href="servlet/cached?<%=id%>">link</a> to download it directly.

<% } %>
