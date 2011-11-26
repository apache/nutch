<%@ page 
  contentType="text/html; charset=UTF-8"
  pageEncoding="UTF-8"

  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.net.*"

  import="org.apache.nutch.html.Entities"
  import="org.apache.nutch.searcher.*"
%><%
  NutchBean bean = NutchBean.get(application);
  // set the character encoding to use when interpreting request values 
  request.setCharacterEncoding("UTF-8");

  bean.LOG.info("query request from " + request.getRemoteAddr());

  // get query from request
  String queryString = request.getParameter("query");
  if (queryString == null)			  
    queryString = "";
  String htmlQueryString = Entities.encode(queryString);
    
  int start = 0;				  // first hit to display
  String startString = request.getParameter("start");
  if (startString != null)
    start = Integer.parseInt(startString);

  int hitsPerPage = 10;				  // number of hits to display
  String hitsString = request.getParameter("hitsPerPage");
  if (hitsString != null)
    hitsPerPage = Integer.parseInt(hitsString);

  int hitsPerSite = 2;                            // max hits per site
  String hitsPerSiteString = request.getParameter("hitsPerSite");
  if (hitsPerSiteString != null)
    hitsPerSite = Integer.parseInt(hitsPerSiteString);
    
  Query query = Query.parse(queryString);

  boolean commercial = false;
  String commercialString = request.getParameter("commercial");
  if (commercialString != null)
    commercial = "true".equals(commercialString);
  if (commercial)
    query.addProhibitedTerm("nc", "cc");
  
  boolean derivatives = false;
  String derivativesString = request.getParameter("derivatives");
  if (derivativesString != null)
    derivatives = "true".equals(derivativesString);
  if (derivatives)
    query.addProhibitedTerm("nd", "cc");

  String format = request.getParameter("format");
  if (format == null) 
    format = "";
  if (!"".equals(format))
    query.addRequiredTerm(format, "cc");

  bean.LOG.info("query: " + query);

  String language =
    ResourceBundle.getBundle("org.nutch.jsp.search", request.getLocale())
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
<i18n:bundle baseName="org.nutch.jsp.search"/>
<html lang="<%= language %>">
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<head>
<title>Creative Commons: <i18n:message key="title"/></title>
<link rel="icon" href="http://creativecommons.org/includes/cc.ico" type="image/ico" />
<link rel="SHORTCUT ICON" href="http://creativecommons.org/includes/cc.ico" />
<jsp:include page="/include/style.html"/>
<base href="<%= base  + "/" + language %>/">
</head>

<body>

<jsp:include page="/include/header.html"/>

<div class="searchbox">
<h1><a href="http://creativecommons.org/">Creative Commons</a> RDF-enhanced search</h1>

<form action="/search.jsp" class="searchform" name="search" method="get">

<input type="text" name="query" value="<%=htmlQueryString%>" class="box"/><br/>
<input type="submit" value="<i18n:message key="search"/>">
<span class="notes">[<a href="help" onclick="window.open('help', 'help', 'width=500,height=300,scrollbars=yes,resizable=yes,toolbar=no,directories=no,location=no,menubar=no,status=yes');return false;">Help</a>]</span><br />

<div class="searchoptions">
<input type="checkbox" name="commercial" value=true
  <%=commercial?"checked":""%> /> I want to make commercial use.
<br/>

<input type="checkbox" name="derivatives" value=true
  <%=derivatives?"checked":""%> /> I want to create derivative works.
<br/>

Format: <select name="format">
<option <%="".equals(format)?"selected":""%> value="">Any</option>
<option <%="audio".equals(format)?"selected":""%> value="audio">Audio</option>
<option <%="image".equals(format)?"selected":""%> value="image">Image</option>
<option <%="interactive".equals(format)?"selected":""%> value="interactive">Interactive</option>
<option <%="text".equals(format)?"selected":""%> value="text">Text</option>
<option <%="video".equals(format)?"selected":""%> value="video" >Video</option>
</select>
</div>
<input type="hidden" name="hitsPerPage" value="<%=hitsPerPage%>">
<input type="hidden" name="hitsPerSite" value="<%=hitsPerSite%>">
</form>
</div>

<div class="resultsbox">

<%
   // perform query
   Hits hits = bean.search(query, start + hitsPerPage, hitsPerSite);
   int end = (int)Math.min(hits.getLength(), start + hitsPerPage);
   int length = end-start;
   Hit[] show = hits.getHits(start, length);
   HitDetails[] details = bean.getDetails(show);
   String[] summaries = bean.getSummary(details, query);

   bean.LOG.info("total hits: " + hits.getTotal());
%>

<i18n:message key="hits">
  <i18n:messageArg value="<%=new Long(start+1)%>"/>
  <i18n:messageArg value="<%=new Long(end)%>"/>
  <i18n:messageArg value="<%=new Long(hits.getTotal())%>"/>
</i18n:message>

<%
  for (int i = 0; i < length; i++) {		  // display the hits
    Hit hit = show[i];
    HitDetails detail = details[i];
    String title = detail.getValue("title");
    String url = detail.getValue("url");
    String summary = summaries[i];
    String id = "idx=" + hit.getIndexNo() + "&id=" + hit.getIndexDocNo();

    if (title == null || title.equals(""))        // use url for docs w/o title
      title = url;
    %>
    <br><br><b>
    <a href="<%=url%>"><%=Entities.encode(title)%></a>
    </b>
    <% if (!"".equals(summary)) { %>
    <br><%=summary%>
    <% } %>
    <br>
    <span class="url"><%=Entities.encode(url)%></span>
    (<a href="/cached.jsp?<%=id%>"><i18n:message key="cached"/></a>)
    (<a href="/explain.jsp?<%=id%>&query=<%=URLEncoder.encode(queryString)%>"><i18n:message key="explain"/></a>)
<!--
    (<a href="/anchors.jsp?<%=id%>"><i18n:message key="anchors"/></a>)
-->
    <% if (hit.moreFromSiteExcluded()) {
    String more =
    "query="+URLEncoder.encode("site:"+hit.getSite()+" "+queryString)
    +"&start="+start+"&hitsPerPage="+hitsPerPage+"&hitsPerSite="+0;%>
    (<a href="/search.jsp?<%=more%>"><i18n:message key="moreFrom"/>
     <%=hit.getSite()%></a>)
    <% } %>
<% } %>

<%
if ((hits.totalIsExact() && end < hits.getTotal()) // more hits to show
    || (!hits.totalIsExact() && (hits.getLength() >= start+hitsPerPage))) {
%>
    <form name="search" action="/search.jsp" method="get">
    <input type="hidden" name="query" value="<%=htmlQueryString%>">
    <input type="hidden" name="start" value="<%=end%>">
    <input type="hidden" name="hitsPerPage" value="<%=hitsPerPage%>">
    <input type="hidden" name="hitsPerSite" value="<%=hitsPerSite%>">
    <input type="hidden" name="commercial" value="<%=commercial%>">
    <input type="hidden" name="derivatives" value="<%=derivatives%>">
    <input type="hidden" name="format" value="<%=format%>">
    <input type="submit" value="<i18n:message key="next"/>">
    </form>
<%
    }

if ((!hits.totalIsExact() && (hits.getLength() < start+hitsPerPage))) {
%>
    <form name="search" action="/search.jsp" method="get">
    <input type="hidden" name="query" value="<%=htmlQueryString%>">
    <input type="hidden" name="hitsPerPage" value="<%=hitsPerPage%>">
    <input type="hidden" name="hitsPerSite" value="0">
    <input type="hidden" name="commercial" value="<%=commercial%>">
    <input type="hidden" name="derivatives" value="<%=derivatives%>">
    <input type="hidden" name="format" value="<%=format%>">
    <input type="submit" value="<i18n:message key="showAllHits"/>">
    </form>
<%
    }
%>

</div>

<br>
<a href="http://www.nutch.org/">
<img border="0" src="/img/poweredbynutch_01.gif">
</a>

<jsp:include page="/include/footer.html"/>

</body>
</html>
