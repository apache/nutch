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
package org.apache.nutch.searcher.response;

import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.Summary;

public class SearchResults {

  private String[] fields;
  private String responseType;
  private String query;
  private String lang;
  private String sort;
  private boolean reverse;
  private boolean withSummary = true;
  private int start;
  private int rows;
  private int end;
  private long totalHits;
  private Hit[] hits;
  private HitDetails[] details;
  private Summary[] summaries;

  public SearchResults() {

  }

  public String[] getFields() {
    return fields;
  }

  public void setFields(String[] fields) {
    this.fields = fields;
  }

  public boolean isWithSummary() {
    return withSummary;
  }

  public void setWithSummary(boolean withSummary) {
    this.withSummary = withSummary;
  }

  public String getResponseType() {
    return responseType;
  }

  public void setResponseType(String responseType) {
    this.responseType = responseType;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getLang() {
    return lang;
  }

  public void setLang(String lang) {
    this.lang = lang;
  }

  public String getSort() {
    return sort;
  }

  public void setSort(String sort) {
    this.sort = sort;
  }

  public boolean isReverse() {
    return reverse;
  }

  public void setReverse(boolean reverse) {
    this.reverse = reverse;
  }

  public int getStart() {
    return start;
  }

  public void setStart(int start) {
    this.start = start;
  }

  public int getRows() {
    return rows;
  }

  public void setRows(int rows) {
    this.rows = rows;
  }

  public int getEnd() {
    return end;
  }

  public void setEnd(int end) {
    this.end = end;
  }

  public long getTotalHits() {
    return totalHits;
  }

  public void setTotalHits(long totalHits) {
    this.totalHits = totalHits;
  }

  public Hit[] getHits() {
    return hits;
  }

  public void setHits(Hit[] hits) {
    this.hits = hits;
  }

  public HitDetails[] getDetails() {
    return details;
  }

  public void setDetails(HitDetails[] details) {
    this.details = details;
  }

  public Summary[] getSummaries() {
    return summaries;
  }

  public void setSummaries(Summary[] summaries) {
    this.summaries = summaries;
  }

}
