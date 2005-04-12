/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.searcher;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.logging.Level;

import javax.servlet.ServletException;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpUtils;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import javax.xml.parsers.*;
import org.w3c.dom.*;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.nutch.html.Entities;
import org.apache.nutch.searcher.*;
import org.apache.nutch.plugin.*;
import org.apache.nutch.clustering.*;
import org.apache.nutch.util.NutchConf;


/** Present search results using A9's OpenSearch extensions to RSS, plus a few
 * Nutch-specific extensions. */   
public class OpenSearchServlet extends HttpServlet {
  private static final String OPENSEARCH_NS =
    "http://a9.com/-/spec/opensearchrss/1.0/";

  private static final String NUTCH_NS =
    "http://www.nutch.org/opensearchrss/1.0/";

  private NutchBean bean;

  public void init(ServletConfig config) throws ServletException {
    try {
      bean = NutchBean.get(config.getServletContext());
    } catch (IOException e) {
      throw new ServletException(e);
    }
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {

    bean.LOG.info("query request from " + request.getRemoteAddr());

    // get parameters from request
    request.setCharacterEncoding("UTF-8");
    String queryString = request.getParameter("query");
    if (queryString == null)
      queryString = "";
    String htmlQueryString = Entities.encode(queryString);

    int start = 0;                                // first hit to display
    String startString = request.getParameter("start");
    if (startString != null)
      start = Integer.parseInt(startString);
    
    int hitsPerPage = 10;                         // number of hits to display
    String hitsString = request.getParameter("hitsPerPage");
    if (hitsString != null)
      hitsPerPage = Integer.parseInt(hitsString);

    int hitsPerSite = 2;                          // max hits per site
    String hitsPerSiteString = request.getParameter("hitsPerSite");
    if (hitsPerSiteString != null)
      hitsPerSite = Integer.parseInt(hitsPerSiteString);

    Query query = Query.parse(queryString);
    bean.LOG.info("query: " + queryString);

    // execute the query
    Hits hits;
    try {
      hits = bean.search(query, start + hitsPerPage, hitsPerSite);
    } catch (IOException e) {
      bean.LOG.log(Level.WARNING, "Search Error", e);
      hits = new Hits(0,new Hit[0]);	
    }

    bean.LOG.info("total hits: " + hits.getTotal());

    // generate xml results
    int end = (int)Math.min(hits.getLength(), start + hitsPerPage);
    int length = end-start;

    Hit[] show = hits.getHits(start, end-start);
    HitDetails[] details = bean.getDetails(show);
    String[] summaries = bean.getSummary(details, query);

    String requestUrl = HttpUtils.getRequestURL(request).toString();
    String base = requestUrl.substring(0, requestUrl.lastIndexOf('/'));

    try {
      Document doc = DocumentBuilderFactory.newInstance()
        .newDocumentBuilder().newDocument();
 
      Element rss = addNode(doc, doc, "rss");
      addAttribute(doc, rss, "version", "2.0");

      Element channel = addNode(doc, rss, "channel");
    
      addNode(doc, channel, "title", "Nutch: " + queryString);
      addNode(doc, channel, "description", "Nutch search results for query: "
              + queryString);
      addNode(doc, channel, "link",
              base+"/search.jsp"
              +"?query="+htmlQueryString
              +"&start="+start
              +"&hitsPerPage="+hitsPerPage
              +"&hitsPerSite="+hitsPerSite);

      addNode(doc, channel, OPENSEARCH_NS, "totalResults", ""+hits.getTotal());
      addNode(doc, channel, OPENSEARCH_NS, "startIndex", ""+start);
      addNode(doc, channel, OPENSEARCH_NS, "itemsPerPage", ""+hitsPerPage);
    
      for (int i = 0; i < length; i++) {
        Hit hit = show[i];
        HitDetails detail = details[i];
        String title = detail.getValue("title");
        String url = detail.getValue("url");
        String id = "idx=" + hit.getIndexNo() + "&id=" + hit.getIndexDocNo();
      
        if (title == null || title.equals(""))    // use url for docs w/o title
          title = url;

        Element item = addNode(doc, channel, "item");

        addNode(doc, item, "title", title);
        addNode(doc, item, "description", summaries[i]);
        addNode(doc, item, "link", url);

        addNode(doc, channel, NUTCH_NS, "cache", base+"/cached.jsp?"+id);
        addNode(doc, channel, NUTCH_NS, "explain", base+"/explain.jsp?"+id
                +"&query="+URLEncoder.encode(queryString));

        if (hit.moreFromSiteExcluded()) {
          addNode(doc, channel, NUTCH_NS, "moreFromSite", base+"/search.jsp"
                  +"?query="
                  +URLEncoder.encode("site:"+hit.getSite()+" "+queryString)
                  +"&hitsPerPage="+hitsPerPage+"&hitsPerSite="+0);
        }
      }

      // dump DOM tree

      DOMSource source = new DOMSource(doc);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();
      transformer.setOutputProperty("indent", "yes");
      StreamResult result = new StreamResult(response.getOutputStream());
      response.setContentType("text/xml");
      transformer.transform(source, result);

    } catch (javax.xml.parsers.ParserConfigurationException e) {
      throw new ServletException(e);
    } catch (javax.xml.transform.TransformerException e) {
      throw new ServletException(e);
    }
      
  }

  private static Element addNode(Document doc, Node parent, String name) {
    Element child = doc.createElement(name);
    parent.appendChild(child);
    return child;
  }

  private static void addNode(Document doc, Node parent,
                              String name, String text) {
    Element child = doc.createElement(name);
    child.appendChild(doc.createTextNode(text));
    parent.appendChild(child);
  }

  private static void addNode(Document doc, Node parent,
                              String nameSpace, String name, String text) {
    Element child = doc.createElementNS(nameSpace, name);
    child.appendChild(doc.createTextNode(text));
    parent.appendChild(child);
  }

  private static void addAttribute(Document doc, Element node,
                                   String name, String value) {
    Attr attribute = doc.createAttribute(name);
    attribute.setValue(value);
    node.getAttributes().setNamedItem(attribute);
  }

}

