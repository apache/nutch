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

/**
 * Helper class to aid when forming search result navigation
 * 
 * 
 */
public class NavigationHelper {

  private int pageStart;

  private int hitsPerPage;

  private long totalHits;

  private boolean totalIsExact;

  public NavigationHelper(int pageStart, int hitsPerPage, long totalHits,
      boolean totalIsExact) {
    this.pageStart = pageStart;
    this.hitsPerPage = hitsPerPage;
    this.totalHits = totalHits;
    this.totalIsExact = totalIsExact;
  }

  protected Boolean hasPrev() {
    return new Boolean(pageStart > 0);
  }

  protected Boolean hasNext() {
    return new Boolean((totalIsExact && pageStart + hitsPerPage < totalHits)
        || (!totalIsExact && totalHits > pageStart + hitsPerPage));

  }

  protected void next() {
    pageStart += hitsPerPage;
  }

  /**
   * Returns offset to next page
   * 
   * @return
   */
  public long getNextStart() {
    return pageStart + hitsPerPage;
  }

  public void prev() {
    pageStart -= hitsPerPage;
  }

  public int getPageNumber() {
    return pageStart / hitsPerPage;
  }

  protected Boolean getShowAllHits() {
    return new Boolean(!totalIsExact && (totalHits <= pageStart + hitsPerPage));
  }

  public long getEnd() {
    return Math.min(totalHits, getNextStart());
  }

}
