/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.webapp.servlet;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.Hits;
import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.Summary;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * Present search results using A9's OpenSearch extensions to RSS, plus a few
 * Nutch-specific extensions.
 */
public class OpenSearchServlet extends NutchHttpServlet {
  private static final int DEFAULT_NUMBER_OF_HITS = 10;

  private static final long serialVersionUID = 1L;

  private static final Map NS_MAP = new HashMap();

  static {
    NS_MAP.put("opensearch", "http://a9.com/-/spec/opensearchrss/1.0/");
    NS_MAP.put("nutch", "http://www.nutch.org/opensearchrss/1.0/");
  }

  private static final Set SKIP_DETAILS = new HashSet();
  static {
    SKIP_DETAILS.add("url"); // redundant with RSS link
    SKIP_DETAILS.add("title"); // redundant with RSS title
  }

  public void init(ServletConfig conf) throws ServletException {
    super.init(conf);
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    NutchBean.LOG.info("query request from " + request.getRemoteAddr());

    // get parameters from request
    request.setCharacterEncoding("UTF-8");
    String queryString = request.getParameter("query");
    if (queryString == null) {
      queryString = "";
    }
    String urlQuery = URLEncoder.encode(queryString, "UTF-8");

    int start = 0; // first hit to display
    String startString = request.getParameter("start");
    if (startString != null) {
      start = Integer.parseInt(startString);
    }

    int hitsPerPage = DEFAULT_NUMBER_OF_HITS; // number of hits to display
    String hitsString = request.getParameter("hitsPerPage");
    if (hitsString != null) {
      hitsPerPage = Integer.parseInt(hitsString);
    }

    String sort = request.getParameter("sort");
    boolean reverse = sort != null
        && "true".equals(request.getParameter("reverse"));

    // De-Duplicate handling. Look for duplicates field and for how many
    // duplicates per results to return. Default duplicates field is 'site'
    // and duplicates per results default is '2'.
    String dedupField = request.getParameter("dedupField");
    if (dedupField == null || dedupField.length() == 0) {
      dedupField = "site";
    }
    int hitsPerDup = 2;
    String hitsPerDupString = request.getParameter("hitsPerDup");
    if (hitsPerDupString != null && hitsPerDupString.length() > 0) {
      hitsPerDup = Integer.parseInt(hitsPerDupString);
    } else {
      // If 'hitsPerSite' present, use that value.
      String hitsPerSiteString = request.getParameter("hitsPerSite");
      if (hitsPerSiteString != null && hitsPerSiteString.length() > 0) {
        hitsPerDup = Integer.parseInt(hitsPerSiteString);
      }
    }

    // Make up query string for use later drawing the 'rss' logo.
    String params = "&hitsPerPage="
        + hitsPerPage
        + (sort == null ? "" : "&sort=" + sort
            + (reverse ? "&reverse=true" : "")
            + (dedupField == null ? "" : "&dedupField=" + dedupField));

    Query query = Query.parse(queryString, getServiceLocator()
        .getConfiguration());
    NutchBean.LOG.info("query: " + queryString);

    // execute the query
    Hits hits;
    try {
      hits = getServiceLocator().getNutchBean().search(query,
          start + hitsPerPage, hitsPerDup, dedupField, sort, reverse);
    } catch (IOException e) {
      NutchBean.LOG.warn("Search Error", e);
      hits = new Hits(0, new Hit[0]);
    }

    NutchBean.LOG.info("total hits: " + hits.getTotal());

    // generate xml results
    int end = (int) Math.min(hits.getLength(), start + hitsPerPage);
    int length = end - start;

    Hit[] show = hits.getHits(start, end - start);
    HitDetails[] details = getServiceLocator().getNutchBean().getDetails(show);
    Summary[] summaries = getServiceLocator().getNutchBean().getSummary(
        details, query);

    String requestUrl = request.getRequestURL().toString();
    String base = requestUrl.substring(0, requestUrl.lastIndexOf('/'));

    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setNamespaceAware(true);
      Document doc = factory.newDocumentBuilder().newDocument();

      Element rss = addNode(doc, doc, "rss");
      addAttribute(doc, rss, "version", "2.0");
      addAttribute(doc, rss, "xmlns:opensearch", (String) NS_MAP
          .get("opensearch"));
      addAttribute(doc, rss, "xmlns:nutch", (String) NS_MAP.get("nutch"));

      Element channel = addNode(doc, rss, "channel");

      addNode(doc, channel, "title", "Nutch: " + queryString);
      addNode(doc, channel, "description", "Nutch search results for query: "
          + queryString);
      addNode(doc, channel, "link", base + "/search.jsp" + "?query=" + urlQuery
          + "&start=" + start + "&hitsPerDup=" + hitsPerDup + params);

      addNode(doc, channel, "opensearch", "totalResults", "" + hits.getTotal());
      addNode(doc, channel, "opensearch", "startIndex", "" + start);
      addNode(doc, channel, "opensearch", "itemsPerPage", "" + hitsPerPage);

      addNode(doc, channel, "nutch", "query", queryString);

      if ((hits.totalIsExact() && end < hits.getTotal()) // more hits to show
          || (!hits.totalIsExact() && (hits.getLength() > start + hitsPerPage))) {
        addNode(doc, channel, "nutch", "nextPage", requestUrl + "?query="
            + urlQuery + "&start=" + end + "&hitsPerDup=" + hitsPerDup + params);
      }

      if ((!hits.totalIsExact() && (hits.getLength() <= start + hitsPerPage))) {
        addNode(doc, channel, "nutch", "showAllHits", requestUrl + "?query="
            + urlQuery + "&hitsPerDup=" + 0 + params);
      }

      for (int i = 0; i < length; i++) {
        Hit hit = show[i];
        HitDetails detail = details[i];
        String title = detail.getValue("title");
        String url = detail.getValue("url");
        String id = "idx=" + hit.getIndexNo() + "&id=" + hit.getIndexDocNo();

        if (title == null || title.equals("")) {
          // use url for docs w/o title
          title = url;
        }

        Element item = addNode(doc, channel, "item");

        addNode(doc, item, "title", title);
        addNode(doc, item, "description", summaries[i].toString());
        addNode(doc, item, "link", url);

        addNode(doc, item, "nutch", "site", hit.getDedupValue());

        addNode(doc, item, "nutch", "cache", base + "/cached.do?" + id);
        addNode(doc, item, "nutch", "explain", base + "/explain.do?" + id
            + "&query=" + urlQuery);

        if (hit.moreFromDupExcluded()) {
          addNode(doc, item, "nutch", "moreFromSite", requestUrl
              + "?query="
              + URLEncoder.encode("site:" + hit.getDedupValue() + " "
                  + queryString, "UTF-8") + "&hitsPerSite=" + 0 + params);
        }

        for (int j = 0; j < detail.getLength(); j++) { // add all from detail
          String field = detail.getField(j);
          if (!SKIP_DETAILS.contains(field)) {
            addNode(doc, item, "nutch", field, detail.getValue(j));
          }
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

  private static void addNode(Document doc, Node parent, String name,
      String text) {
    Element child = doc.createElement(name);
    child.appendChild(doc.createTextNode(text));
    parent.appendChild(child);
  }

  private static void addNode(Document doc, Node parent, String ns,
      String name, String text) {
    Element child = doc.createElementNS((String) NS_MAP.get(ns), ns + ":"
        + name);
    child.appendChild(doc.createTextNode(text));
    parent.appendChild(child);
  }

  private static void addAttribute(Document doc, Element node, String name,
      String value) {
    Attr attribute = doc.createAttribute(name);
    attribute.setValue(value);
    node.getAttributes().setNamedItem(attribute);
  }

}
