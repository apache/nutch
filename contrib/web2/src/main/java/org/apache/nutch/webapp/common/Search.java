/*
 * Copyright 2006 The Apache Software Foundation
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
package org.apache.nutch.webapp.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.util.LogFormatter;
import org.apache.nutch.html.Entities;
import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.Hits;
import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.searcher.Query;

/**
 * Search is a bean that represents an ongoing search.
 * 
 * After search searchBean that represents the search user doing (also the
 * results) might be a good candidate for caching ?
 * 
 */
public class Search {

  public static Logger LOG = LogFormatter.getLogger(Search.class.getName());

  String queryString;

  String htmlQueryString;

  Query query;

  int maxHits;

  int startOffset;

  int hitsPerDup;

  int hitsPerPage;

  String sortColumn;

  boolean sortDesc;

  Hits hits;

  Hit[] show;

  HitDetails[] details;

  String[] summaries;

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
  public void performSearch(NutchBean bean) {

    try {
      hits = bean.search(getQuery(), getStartOffset() + getMaxHits(),
          getHitsPerSite(), getDupField(), getSortColumn(), isSortDesc());
    } catch (IOException e) {
      hits = new Hits(0, new Hit[0]);
    }

    LOG.info("form:");
    LOG.info(locator.getSearchForm().toString());
    LOG.info("performing search");

    int realEnd = (int) Math.min(hits.getLength(), getStartOffset()
        + getMaxHits());

    int endOffset=hits.getLength();
    
    show = hits.getHits(getStartOffset(), realEnd - getStartOffset());

    
    
    navigationHelper = new NavigationHelper(startOffset, endOffset, hitsPerPage, hits
        .getTotal(), hits.totalIsExact());

    // set offset to next page to form so it get's to ui
    if (navigationHelper.hasNext()) {
      form.setValue(SearchForm.NAME_START, Long.toString(navigationHelper
          .getNextPageStart()));
    }

    try {
      details = bean.getDetails(show);
      summaries = bean.getSummary(details, getQuery());
    } catch (IOException e) {
      LOG.info("Error getting hit information:" + e);
      e.printStackTrace();
    }
  }

  /**
   * gets the results of search to display
   * 
   * @return
   */
  public List getResults() {
    if (results == null) {
      results = new ArrayList(details.length);
      for (int i = 0; i < details.length; i++) {
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

  public Search(ServiceLocator locator) {
    this.locator = locator;
    Preferences prefs = locator.getPreferences();
    form = locator.getSearchForm();

    queryString = form.getValueString(SearchForm.NAME_QUERYSTRING);
    if (queryString == null)
      queryString = "";
    htmlQueryString = Entities.encode(queryString);

    startOffset = parseInt(form.getValueString(SearchForm.NAME_START), 0);
    hitsPerPage = parseInt(form.getValueString(SearchForm.NAME_HITSPERPAGE),
        prefs.getInt(Preferences.KEY_RESULTS_PER_PAGE, 10));
    hitsPerDup = parseInt(form.getValueString(SearchForm.NAME_HITSPERDUP), prefs.getInt(
        Preferences.KEY_HITS_PER_DUP, 2));

    maxHits = hitsPerPage;

    sortColumn = form.getValueString(SearchForm.NAME_SORTCOLUMN);

    sortDesc = (sortColumn != null && "true".equals(form
        .getValueString(SearchForm.NAME_SORTREVERSE)));

    dupField = form.getValueString(SearchForm.NAME_DUPCOLUMN);
    if (dupField == null)
      dupField = "site";

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
  public int getHitsPerSite() {
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
   * @return Returns the maxHits.
   */
  public int getMaxHits() {
    return maxHits;
  }

  /**
   * @param maxHits
   *          The maxHits to set.
   */
  protected void setMaxHits(int maxHits) {
    this.maxHits = maxHits;
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
  protected void setQuery(Query query) {
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
   * Returns true if sort is descending
   * 
   * @return Returns sort order
   */
  public boolean isSortDesc() {
    return sortDesc;
  }

  /**
   * Set sort order
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
  public String[] getSummaries() {
    return summaries;
  }

  /**
   * @param summaries
   *          The summaries to set.
   */
  protected void setSummaries(String[] summaries) {
    this.summaries = summaries;
  }

  /**
   * returns start, end, total, used for printing message on search page, this
   * is why offset is +1'd
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
   * 
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
   * return boolean if there are results
   * 
   * @return
   */
  public boolean getHasResults() {
    return hits.getTotal() > 0;
  }

  public boolean getIsSearch() {
    return queryString != null && !queryString.trim().equals("");
  }
}
