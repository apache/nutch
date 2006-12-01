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

/**
 * Helper class to aid when forming search result navigation.
 */
public class NavigationHelper {

  private int start;

  private int hitsPerPage;

  private long totalHits;

  private boolean totalIsExact;

  private int end;

  /**
   * Construct a new NavigationHelper
   * @param start start index 
   * @param hitsPerPage 
   * @param length total number of hits
   * @param totalIsExact total number of hits is exact (@see org.apache.nutch.searcher.Hits#setTotalIsExact(boolean))
   */
  public NavigationHelper(int start, int end, int hitsPerPage, long length,
      boolean totalIsExact) {
    this.start = start;
    this.end = end;
    this.hitsPerPage = hitsPerPage;
    this.totalHits = length;
    this.totalIsExact = totalIsExact;
  }

  /**
   * Is there a previous page available.
   *
   * @return
   */
  protected boolean hasPrev() {
    return start > 0;
  }

  /**
   * Is there a next page available
   * @return
   */
  protected boolean hasNext() {
    return end < totalHits && (!getShowAllHits());
  }

  /**
   * Proceed to next page.
   */
  protected void nextPage() {
    start += hitsPerPage;
  }

  /**
   * Returns offset to next page.
   *
   * @return
   */
  public long getNextPageStart() {
    return start + hitsPerPage;
  }

  /**
   * Proceed to previous page.
   */
  public void prev() {
    start -= hitsPerPage;
  }

  /**
   * Get a page number.
   * @return
   */
  public int getPageNumber() {
    return start / hitsPerPage;
  }

  /**
   * @return
   */
  protected boolean getShowAllHits() {
    return !totalIsExact && (start + hitsPerPage > end);
  }

  /**
   * Returns the number of last hit on page.
   * @return
   */
  public long getEnd() {
    return Math.min(totalHits, getNextPageStart());
  }

}
