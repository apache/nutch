/*
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
package org.apache.nutch.webapp.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.html.Entities;
import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.Hits;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.Summary;

/**
 * Search is a bean that represents an ongoing search.
 *
 * After search searchBean that represents the search user doing (also the
 * results) might be a good candidate for caching ?
 *
 */
public class Search implements Writable {

  private static final long serialVersionUID = 1L;

  public static final String REQ_ATTR_SEARCH="nutchSearch";
  public static final Log LOG = LogFactory.getLog(Search.class);

  String queryString;

  String htmlQueryString;

  Query query;

  int startOffset;

  int hitsPerDup;

  // Number of results per page
  int hitsPerPage;

  // Number of hits required, for example clustering plugin might require more
  // than hitsPerPage hits
  int hitsRequired=0;

  String sortColumn;

  boolean sortDesc;

  Hits hits;

  Hit[] show;

  HitDetails[] details;

  Summary[] summaries;

  ArrayList results = null;

  NavigationHelper navigationHelper;

  ServiceLocator locator;

  SearchForm form;

  String dupField;

  /**
   * Perform search described by this bean
   *
   * @param bean
   */
  public void performSearch() {

    LOG.info("performing search, requiring:" + getHitsRequired());

    try {
      LOG.info("query:" + getQuery());
      LOG.info("endOffset:" + getStartOffset() + getHitsRequired());
      LOG.info("hitsPerDup:" + getHitsPerDup());
      LOG.info("dupField:" + getDupField());
      LOG.info("sortField:" + getSortColumn());
      LOG.info("descending:" + isSortDesc());

      hits = locator.getNutchBean().search(getQuery(),
          getStartOffset() + getHitsRequired(), getHitsPerDup(), getDupField(),
          getSortColumn(), isSortDesc());
    } catch (IOException e) {
      hits = new Hits(0, new Hit[0]);
    }

    int realEnd = (int) Math.min(hits.getLength(), getStartOffset()
        + getHitsRequired());

    init();
    show = hits.getHits(getStartOffset(), realEnd - getStartOffset());

    try {
      details = locator.getNutchBean().getDetails(show);
      summaries = locator.getNutchBean().getSummary(details, getQuery());
    } catch (IOException e) {
      LOG.info("Error getting hit information:" + e);
      e.printStackTrace();
    }
  }

  public void init(){
    int endOffset=hits.getLength();

    navigationHelper = new NavigationHelper(startOffset, endOffset, hitsPerPage, hits
        .getTotal(), hits.totalIsExact());

    // set offset to next page to form so it get's to ui
    if (navigationHelper.hasNext()) {
      form.setValue(SearchForm.NAME_START, Long.toString(navigationHelper
          .getNextPageStart()));
    }
  }

  /**
   * Gets the results of search to display.
   *
   * @return
   */
  public List getResults() {

    int len=Math.min(details.length,getHitsPerPage());

    if (results == null) {
      results = new ArrayList(len);
      for (int i = 0; i < len; i++) {
        results.add(new SearchResultBean(this, show[i], details[i],
            summaries[i]));
      }
    }
    return results;
  }

  protected int parseInt(String value, int defaultValue) {
    int ret = defaultValue;
    if (value != null) {
      try {
        ret = Integer.parseInt(value);
      } catch (Exception e) {
        // ignore
      }
    }
    return ret;
  }

  public Search(){

  }

  public Search(ServiceLocator locator) {
    this.locator = locator;
    Preferences prefs = locator.getPreferences();
    form = locator.getSearchForm();

    queryString = form.getValueString(SearchForm.NAME_QUERYSTRING);
    if (queryString == null) {
      queryString = "";
    }
    htmlQueryString = Entities.encode(queryString);

    startOffset = parseInt(form.getValueString(SearchForm.NAME_START), 0);
    hitsPerPage = parseInt(form.getValueString(SearchForm.NAME_HITSPERPAGE),
        prefs.getInt(Preferences.KEY_RESULTS_PER_PAGE, 10));
    hitsPerDup = parseInt(form.getValueString(SearchForm.NAME_HITSPERDUP),
        prefs.getInt(
        Preferences.KEY_HITS_PER_DUP, 2));

    sortColumn = form.getValueString(SearchForm.NAME_SORTCOLUMN);

    sortDesc = (sortColumn != null && "true".equals(form
        .getValueString(SearchForm.NAME_SORTREVERSE)));

    dupField = form.getValueString(SearchForm.NAME_DUPCOLUMN);
    if (dupField == null) {
      dupField = "site";
    }

    try {
      query = Query.parse(queryString, locator.getConfiguration());
    } catch (IOException e) {
      LOG.info("Error parsing query:" + e);
      e.printStackTrace();
    }

  }

  /**
   * @return Returns the hitsPerPage.
   */
  public int getHitsPerPage() {
    return hitsPerPage;
  }

  /**
   * @param hitsPerPage
   *          The hitsPerPage to set.
   */
  protected void setHitsPerPage(int hitsPerPage) {
    this.hitsPerPage = hitsPerPage;
  }

  /**
   * @return Returns the hitsPerSite.
   */
  public int getHitsPerDup() {
    return hitsPerDup;
  }

  /**
   * @param hitsPerSite
   *          The hitsPerSite to set.
   */
  protected void setHitsPerSite(int hitsPerSite) {
    this.hitsPerDup = hitsPerSite;
  }

  /**
   * @return Returns the query.
   */
  public Query getQuery() {
    return query;
  }

  /**
   * @param query
   *          The query to set.
   */
  public void setQuery(Query query) {
    this.query = query;
  }

  /**
   * @return Returns the queryString.
   */
  public String getQueryString() {
    return queryString;
  }

  /**
   * @param queryString
   *          The queryString to set.
   */
  protected void setQueryString(String queryString) {
    this.queryString = queryString;
  }

  /**
   * Returns true if sort is descending.
   *
   * @return Returns sort order
   */
  public boolean isSortDesc() {
    return sortDesc;
  }

  /**
   * Set sort order.
   *
   * @param sortAsc
   *          The sortAsc to set.
   */
  protected void setSortDesc(boolean sortAsc) {
    this.sortDesc = sortAsc;
  }

  /**
   * @return Returns the sortColumn.
   */
  public String getSortColumn() {
    return sortColumn;
  }

  /**
   * @param sortColumn
   *          The sortColumn to set.
   */
  protected void setSortColumn(String sortColumn) {
    this.sortColumn = sortColumn;
  }

  /**
   * @return Returns the startOffset.
   */
  public int getStartOffset() {
    return startOffset;
  }

  /**
   * @param startOffset
   *          The startOffset to set.
   */
  protected void setStartOffset(int startOffset) {
    this.startOffset = startOffset;
  }

  /**
   * @return Returns the htmlQueryString.
   */
  public String getHtmlQueryString() {
    return htmlQueryString;
  }

  /**
   * @param htmlQueryString
   *          The htmlQueryString to set.
   */
  protected void setHtmlQueryString(String htmlQueryString) {
    this.htmlQueryString = htmlQueryString;
  }

  /**
   * @return Returns the details.
   */
  public HitDetails[] getDetails() {
    return details;
  }

  /**
   * @param details
   *          The details to set.
   */
  protected void setDetails(HitDetails[] details) {
    this.details = details;
  }

  /**
   * @return Returns the hits.
   */
  public Hits getHits() {
    return hits;
  }

  /**
   * @param hits
   *          The hits to set.
   */
  protected void setHits(Hits hits) {
    this.hits = hits;
  }

  /**
   * @return Returns the show.
   */
  public Hit[] getShow() {
    return show;
  }

  /**
   * @param show
   *          The show to set.
   */
  protected void setShow(Hit[] show) {
    this.show = show;
  }

  /**
   * @return Returns the summaries.
   */
  public Summary[] getSummaries() {
    return summaries;
  }

  /**
   * @param summaries
   *          The summaries to set.
   */
  protected void setSummaries(Summary[] summaries) {
    this.summaries = summaries;
  }

  /**
   * returns start, end, total, used for printing message on search page, this
   * is why offset is +1'd.
   */
  public String[] getResultInfo() {
    return new String[] { Long.toString(startOffset + 1),
        Long.toString(navigationHelper.getEnd()),
        Long.toString(hits.getTotal()), getHtmlQueryString() };
  }

  /**
   * @return true if more results available
   */
  public boolean getHasNextPage() {
    return navigationHelper.hasNext();
  }

  /**
   * @return true if previous page if available
   */
  public boolean getHasPrevPage() {
    return navigationHelper.hasPrev();
  }

  public String getDupField() {
    return dupField;
  }

  protected SearchForm getForm() {
    return form;
  }

  /**
   * @return
   */
  public List getFormProperties() {
    return form.getActive();
  }

  public boolean getShowAllHits() {
    if(navigationHelper.getShowAllHits()){
      //remove start parameter from form
      getForm().remove(SearchForm.NAME_START);
      //add hitsPerDup=0
      getForm().setValue(SearchForm.NAME_HITSPERDUP,"0");
    }
    return navigationHelper.getShowAllHits();
  }

  /**
   * Return true if there are results.
   *
   * @return
   */
  public boolean getHasResults() {
    return hits.getTotal() > 0;
  }

  public boolean getIsSearch() {
    return queryString != null && !queryString.trim().equals("");
  }

  /**
   * Return number of hits required, if not specified defaults to HitsPerPage.
   * @return
   */
  public int getHitsRequired() {
    if (hitsRequired != 0) {
      return hitsRequired;
    }
    return getHitsPerPage();
  }

  /**
   * Set number of hits required.
   * @param hitsRequired
   */
  public void setHitsRequired(int hitsRequired) {
    this.hitsRequired = hitsRequired;
  }

  /**
   * Launch search.
   */
  public void launchSearch() {
    BaseSearch bs = new BaseSearch(locator);
    bs.doSearch();
  }

  public void write(DataOutput out) throws IOException {
    LOG.info("writing hits");
    hits.write(out);
    out.writeInt(show.length);

    for (int i = 0; i < show.length; i++) {
      show[i].write(out);
    }

    out.writeInt(details.length);
    for (int i = 0; i < details.length; i++) {
      details[i].write(out);
    }

    out.writeInt(summaries.length);
    for (int i = 0; i < summaries.length; i++) {
      summaries[i].write(out);
    }

  }

  public void readFields(DataInput in) throws IOException {
    hits = new Hits();
    hits.readFields(in);
    int showlength = in.readInt();
    show = new Hit[showlength];
    for (int i = 0; i < showlength; i++) {
      show[i] = new Hit();
      show[i].readFields(in);
    }

    int detailsLength = in.readInt();
    details = new HitDetails[detailsLength];
    for (int i = 0; i < detailsLength; i++) {
      details[i] = new HitDetails();
      details[i].readFields(in);
    }

    int summariesLength = in.readInt();
    summaries = new Summary[summariesLength];
    for (int i = 0; i < summariesLength; i++) {
      summaries[i] = new Summary();
      summaries[i].readFields(in);
    }
  }
  
}
