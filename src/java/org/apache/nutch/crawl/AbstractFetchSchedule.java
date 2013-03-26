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

package org.apache.nutch.crawl;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.storage.WebPage;

/**
 * This class provides common methods for implementations of
 * {@link FetchSchedule}.
 *
 * @author Andrzej Bialecki
 */
public abstract class AbstractFetchSchedule
extends Configured
implements FetchSchedule {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFetchSchedule.class);

  protected int defaultInterval;
  protected int maxInterval;

  private static final Set<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.FETCH_TIME);
    FIELDS.add(WebPage.Field.RETRIES_SINCE_FETCH);
    FIELDS.add(WebPage.Field.FETCH_INTERVAL);
  }

  public AbstractFetchSchedule() {
    super(null);
  }

  public AbstractFetchSchedule(Configuration conf) {
    super(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) return;
    int oldDefaultInterval = conf.getInt("db.default.fetch.interval", 0);
    defaultInterval = conf.getInt("db.fetch.interval.default", 0);
    if (oldDefaultInterval > 0 && defaultInterval == 0) defaultInterval = oldDefaultInterval * SECONDS_PER_DAY;
    int oldMaxInterval = conf.getInt("db.max.fetch.interval", 0);
    maxInterval = conf.getInt("db.fetch.interval.max", 0 );
    if (oldMaxInterval > 0 && maxInterval == 0) maxInterval = oldMaxInterval * FetchSchedule.SECONDS_PER_DAY;
    LOG.info("defaultInterval=" + defaultInterval);
    LOG.info("maxInterval=" + maxInterval);
  }
  
  /**
   * Initialize fetch schedule related data. Implementations should at least
   * set the <code>fetchTime</code> and <code>fetchInterval</code>. The default
   * implementation sets the <code>fetchTime</code> to now, using the
   * default <code>fetchInterval</code>.
   *
   * @param url URL of the page.
   * @param page
   */
  @Override
  public void initializeSchedule(String url, WebPage page) {
    page.setFetchTime(System.currentTimeMillis());
    page.setFetchInterval(defaultInterval);
    page.setRetriesSinceFetch(0);
  }

  /**
   * Sets the <code>fetchInterval</code> and <code>fetchTime</code> on a
   * successfully fetched page. NOTE: this implementation resets the
   * retry counter - extending classes should call super.setFetchSchedule() to
   * preserve this behavior.
   */
  @Override
  public void setFetchSchedule(String url, WebPage page,
          long prevFetchTime, long prevModifiedTime,
          long fetchTime, long modifiedTime, int state) {
    page.setRetriesSinceFetch(0);
  }

  /**
   * This method specifies how to schedule refetching of pages
   * marked as GONE. Default implementation increases fetchInterval by 50%,
   * and if it exceeds the <code>maxInterval</code> it calls
   * {@link #forceRefetch(Text, CrawlDatum, boolean)}.
   * @param url URL of the page
   * @param page
   * @return adjusted page information, including all original information.
   * NOTE: this may be a different instance than {@param datum}, but
   * implementations should make sure that it contains at least all
   * information from {@param datum}.
   */
  @Override
  public void setPageGoneSchedule(String url, WebPage page,
          long prevFetchTime, long prevModifiedTime, long fetchTime) {
    // no page is truly GONE ... just increase the interval by 50%
    // and try much later.
    int newFetchInterval = (int) (page.getFetchInterval() * 1.5f);
    page.setFetchInterval(newFetchInterval);
    page.setFetchTime(fetchTime + newFetchInterval * 1000L);
    if (maxInterval < newFetchInterval) forceRefetch(url, page, false);
  }

  /**
   * This method adjusts the fetch schedule if fetching needs to be
   * re-tried due to transient errors. The default implementation
   * sets the next fetch time 1 day in the future and increases
   * the retry counter.
   * @param url URL of the page
   * @param page
   * @param prevFetchTime previous fetch time
   * @param prevModifiedTime previous modified time
   * @param fetchTime current fetch time
   */
  @Override
  public void setPageRetrySchedule(String url, WebPage page,
          long prevFetchTime, long prevModifiedTime, long fetchTime) {
    page.setFetchTime(fetchTime + SECONDS_PER_DAY * 1000L);
    page.setRetriesSinceFetch(page.getRetriesSinceFetch() + 1);
  }

  /**
   * This method return the last fetch time of the CrawlDatum
   * @return the date as a long.
   */
  @Override
  public long calculateLastFetchTime(WebPage page) {
    return page.getFetchTime() - page.getFetchInterval() * 1000L;
  }

  /**
   * This method provides information whether the page is suitable for
   * selection in the current fetchlist. NOTE: a true return value does not
   * guarantee that the page will be fetched, it just allows it to be
   * included in the further selection process based on scores. The default
   * implementation checks <code>fetchTime</code>, if it is higher than the
   * {@param curTime} it returns false, and true otherwise. It will also
   * check that fetchTime is not too remote (more than <code>maxInterval</code),
   * in which case it lowers the interval and returns true.
   * @param url URL of the page
   * @param page
   * @param curTime reference time (usually set to the time when the
   * fetchlist generation process was started).
   * @return true, if the page should be considered for inclusion in the current
   * fetchlist, otherwise false.
   */
  @Override
  public boolean shouldFetch(String url, WebPage page, long curTime) {
    // pages are never truly GONE - we have to check them from time to time.
    // pages with too long fetchInterval are adjusted so that they fit within
    // maximum fetchInterval (batch retention period).
    long fetchTime = page.getFetchTime();
    if (fetchTime - curTime > maxInterval * 1000L) {
      if (page.getFetchInterval() > maxInterval) {
        page.setFetchInterval(Math.round(maxInterval * 0.9f));
      }
      page.setFetchTime(curTime);
    }
    return fetchTime <= curTime;
  }

  /**
   * This method resets fetchTime, fetchInterval, modifiedTime,
   * retriesSinceFetch and page signature, so that it forces refetching.
   * @param url URL of the page
   * @param page
   * @param asap if true, force refetch as soon as possible - this sets
   * the fetchTime to now. If false, force refetch whenever the next fetch
   * time is set.
   */
  @Override
  public void forceRefetch(String url, WebPage page, boolean asap) {
    // reduce fetchInterval so that it fits within the max value
    if (page.getFetchInterval() > maxInterval)
      page.setFetchInterval(Math.round(maxInterval * 0.9f));
    page.setStatus(CrawlStatus.STATUS_UNFETCHED);
    page.setRetriesSinceFetch(0);
    // TODO: row.setSignature(null) ??
    page.setModifiedTime(0L);
    if (asap) page.setFetchTime(System.currentTimeMillis());
  }


  public Set<WebPage.Field> getFields() {
    return FIELDS;
  }
}
