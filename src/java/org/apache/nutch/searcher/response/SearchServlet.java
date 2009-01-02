package org.apache.nutch.searcher.response;

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.Hits;
import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.Summary;
import org.apache.nutch.util.NutchConfiguration;

/**
 * Servlet that allows returning search results in multiple different formats
 * through a ResponseWriter Nutch extension point.
 * 
 * @see org.apache.nutch.searcher.response.ResponseWriter
 */
public class SearchServlet
  extends HttpServlet {

  public static final Log LOG = LogFactory.getLog(SearchServlet.class);
  private NutchBean bean;
  private Configuration conf;
  private ResponseWriters writers;

  private String defaultRespType = "xml";
  private String defaultLang = null;
  private int defaultNumRows = 10;
  private String defaultDedupField = "site";
  private int defaultNumDupes = 1;

  public static final String RESPONSE_TYPE = "rt";
  public static final String QUERY = "query";
  public static final String LANG = "lang";
  public static final String START = "start";
  public static final String ROWS = "rows";
  public static final String SORT = "sort";
  public static final String REVERSE = "reverse";
  public static final String DEDUPE = "ddf";
  public static final String NUM_DUPES = "dupes";
  public static final String SUMMARY = "summary";
  public static final String FIELDS = "field";

  /**
   * Initializes servlet configuration default values.  Gets NutchBean and 
   * ResponseWriters.
   */
  public void init(ServletConfig config)
    throws ServletException {

    // set sensible defaults for response writer values and cache NutchBean.
    // Also get and cache all ResponseWriter implementations.
    super.init(config);
    try {
      this.conf = NutchConfiguration.get(config.getServletContext());
      this.defaultRespType = conf.get("search.response.default.type", "xml");
      this.defaultLang = conf.get("search.response.default.lang");
      this.defaultNumRows = conf.getInt("search.response.default.numrows", 10);
      this.defaultDedupField = conf.get("search.response.default.dedupfield",
        "site");
      this.defaultNumDupes = conf.getInt("search.response.default.numdupes", 1);
      bean = NutchBean.get(config.getServletContext(), this.conf);
      writers = new ResponseWriters(conf);
    }
    catch (IOException e) {
      throw new ServletException(e);
    }
  }

  /**
   * Forwards all responses to doGet.
   */
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    doGet(request, response);
  }

  /**
   * Handles all search requests.  Gets parameter input.  Does the search and 
   * gets Hits, details, and summaries.  Passes off to ResponseWriter classes
   * to writer different output formats directly to HttpServletResponse.
   */
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {

    if (NutchBean.LOG.isInfoEnabled()) {
      NutchBean.LOG.info("Query request from " + request.getRemoteAddr());
    }

    // get the response type, used to call the correct ResponseWriter
    String respType = RequestUtils.getStringParameter(request, RESPONSE_TYPE,
      defaultRespType);
    ResponseWriter writer = writers.getResponseWriter(respType);
    if (writer == null) {
      throw new IOException("Unknown response type " + respType);
    }

    // get the query
    String query = RequestUtils.getStringParameter(request, QUERY);
    if (StringUtils.isBlank(query)) {
      throw new IOException("Query cannot be empty!");
    }
    
    // get the language from parameter, then request, then finally configuration
    String lang = RequestUtils.getStringParameter(request, LANG);
    if (StringUtils.isBlank(lang)) {
      lang = request.getLocale().getLanguage();
      if (StringUtils.isBlank(lang)) {
        lang = defaultLang;
      }
    }

    // get various other search parameters, fields allows only returning a 
    // given set of fields
    boolean withSummary = RequestUtils.getBooleanParameter(request, SUMMARY,
      true);
    String sort = RequestUtils.getStringParameter(request, SORT);
    int start = RequestUtils.getIntegerParameter(request, START, 0);
    int rows = RequestUtils.getIntegerParameter(request, ROWS, defaultNumRows);
    boolean reverse = RequestUtils.getBooleanParameter(request, REVERSE, false);
    String dedup = RequestUtils.getStringParameter(request, DEDUPE,
      defaultDedupField);
    int numDupes = RequestUtils.getIntegerParameter(request, NUM_DUPES,
      defaultNumDupes);
    String[] fields = request.getParameterValues(FIELDS);

    // parse out the query
    Query queryObj = Query.parse(query, lang, this.conf);
    if (NutchBean.LOG.isInfoEnabled()) {
      NutchBean.LOG.info("query: " + query);
      NutchBean.LOG.info("lang: " + lang);
    }

    // search and return hits
    Hits hits;
    try {
      hits = bean.search(queryObj, start + rows, numDupes, dedup, sort, reverse);
    }
    catch (IOException e) {
      if (NutchBean.LOG.isWarnEnabled()) {
        NutchBean.LOG.warn("Search Error", e);
      }
      hits = new Hits(0, new Hit[0]);
    }

    // get the total number of hits, the hits to show, and the hit details
    long totalHits = hits.getTotal();
    int end = (int)Math.min(hits.getLength(), start + rows);
    int numHits = (end > start) ? (end - start) : 0;
    Hit[] show = hits.getHits(start, numHits);
    HitDetails[] details = bean.getDetails(show);

    // setup the SearchResults object, used in response writing
    SearchResults results = new SearchResults();
    results.setResponseType(respType);
    results.setQuery(query);
    results.setLang(lang);
    results.setSort(sort);
    results.setReverse(reverse);
    results.setStart(start);
    results.setRows(rows);
    results.setEnd(end);
    results.setTotalHits(totalHits);
    results.setHits(show);
    results.setDetails(details);

    // are we returning summaries with results, if not avoid network hit
    if (withSummary) {
      Summary[] summaries = bean.getSummary(details, queryObj);
      results.setSummaries(summaries);
      results.setWithSummary(true);
    }
    else {
      results.setWithSummary(false);
    }

    // set return fields if any specified, if not all fields are returned
    if (fields != null && fields.length > 0) {
      results.setFields(fields);
    }

    // call the response writer to write out content to HttpResponse directly
    writer.writeResponse(results, request, response);
  }
}
