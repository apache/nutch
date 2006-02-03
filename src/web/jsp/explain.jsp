<%@ page 
  session="false"
  contentType="text/html; charset=UTF-8"
  pageEncoding="UTF-8" 

  import="java.io.*"
  import="java.util.*"
  import="org.apache.nutch.searcher.*"
  import="org.apache.nutch.util.NutchConf"
%><%
  NutchConf nutchConf = (NutchConf) application.getAttribute(NutchConf.class.getName());
  if (nutchConf == null) {
    nutchConf = new NutchConf();
    application.setAttribute(NutchConf.class.getName(), nutchConf);
  }
  NutchBean bean = NutchBean.get(application, nutchConf);
  // set the character encoding to use when interpreting request values 
  request.setCharacterEncoding("UTF-8");
  bean.LOG.info("explain request from " + request.getRemoteAddr());
  Hit hit = new Hit(Integer.parseInt(request.getParameter("idx")),
                    Integer.parseInt(request.getParameter("id")));
  HitDetails details = bean.getDetails(hit);
  Query query = Query.parse(request.getParameter("query"), nutchConf);
  String language =
    ResourceBundle.getBundle("org.nutch.jsp.explain", request.getLocale())
    .getLocale().getLanguage();
  String requestURI = HttpUtils.getRequestURL(request).toString();
  String base = requestURI.substring(0, requestURI.lastIndexOf('/'));
%><!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<%
  // To prevent the character encoding declared with 'contentType' page
  // directive from being overriden by JSTL (apache i18n), we freeze it
  // by flushing the output buffer. 
  // see http://java.sun.com/developer/technicalArticles/Intl/MultilingualJSP/
  out.flush();
%>
<%@ taglib uri="http://jakarta.apache.org/taglibs/i18n" prefix="i18n" %>
<i18n:bundle baseName="org.nutch.jsp.explain"/>
<html lang="<%= language %>">
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<head>
<title>Nutch: <i18n:message key="title"/></title>
<jsp:include page="/include/style.html"/>
<base href="<%= base  + "/" + language %>/">
</head>

<body>

<jsp:include page="<%= language + "/include/header.html"%>"/>

<h3><i18n:message key="page"/></h3>

<%=bean.getDetails(hit).toHtml()%>

<h3><i18n:message key="scoreForQuery">
  <i18n:messageArg value="<%=query%>"/>
</i18n:message>
</h3>

<%=bean.getExplanation(query, hit)%>

<jsp:include page="/include/footer.html"/>

</body>     
</html>
