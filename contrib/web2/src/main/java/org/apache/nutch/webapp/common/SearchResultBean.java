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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.nutch.html.Entities;
import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.Summary;

/**
 * SearchResultBean contains information about one search result in easily
 * accessible form.
 */
public class SearchResultBean {

  Hit hit;

  Summary summary;

  HitDetails details;

  Search search;

  public SearchResultBean(Search search, Hit hit, HitDetails details,
      Summary summary) {
    this.search = search;
    this.hit = hit;
    this.details = details;
    this.summary = summary;
  }

  /**
   * Url of search result.
   *
   * @return
   */
  public String getUrl() {
    return details.getValue("url");
  }

  /**
   * Title of search result.
   *
   * @return
   */
  public String getTitle() {
    String title = details.getValue("title");
    return title == null || title.equals("") ? getUrl() : title;
  }

  /**
   * Id of search result.
   *
   * @return
   */
  public String getId() {
    return "idx=" + hit.getIndexNo() + "&id=" + hit.getIndexDocNo();
  }

  /**
   * Summary of search result.
   *
   * @return
   */
  public String getSummary() {
    return summary.toHtml(true);
  }

  /**
   * Url of search result in encoded form.
   *
   * @return
   */
  public String getEncodedUrl() {
    return Entities.encode(getUrl());
  }

  /**
   * Title of search result in encoded form.
   *
   * @return
   */
  public String getEncodedTitle() {
    return Entities.encode(getTitle());
  }

  /**
   * "more" url this should be replaced with help of SearchForm.
   *
   * @return
   *
   */
  public String getMoreUrl() throws UnsupportedEncodingException {
    String more = "";

    more = "query="
        + URLEncoder.encode(search.getDupField() + ":" + hit.getDedupValue()
            + " " + search.getQueryString(), "UTF8")
        + "&" + SearchForm.NAME_HITSPERDUP  + "=0";

    return more;
  }

  /**
   * Dedup field name.
   *
   * @return
   */
  public String getDedupValue() {
    return hit.getDedupValue();
  }

  /**
   * QueryString in encoded form.
   *
   * @return
   */
  public String getUrlEncodedQuery() {
    try {
      if (search.getQueryString() != null) {
        return URLEncoder.encode(search.getQueryString(), "UTF-8");
      }
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return search.queryString != null ? search.queryString : "";
  }

  /**
   * HitDetails of search result.
   *
   * @return
   */
  public HitDetails getDetails() {
    return details;
  }

  /**
   * Hit of search result.
   *
   * @return
   */
  public Hit getHit() {
    return hit;
  }

  /**
   * Boolean indicating that there are other, lower-scoring hits with the same
   * dedup value.
   *
   * @see Hit#moreFromDupExcluded()
   *
   * @return true if more dups available
   */
  public boolean getHasMore() {
    return hit.moreFromDupExcluded();
  }
}
