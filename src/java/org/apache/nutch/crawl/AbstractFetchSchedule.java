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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.util.hbase.HbaseColumn;
import org.apache.nutch.util.hbase.WebTableColumns;
import org.apache.nutch.util.hbase.WebTableRow;

/**
 * This class provides common methods for implementations of
 * {@link FetchSchedule}.
 * 
 * @author Andrzej Bialecki
 */
public abstract class AbstractFetchSchedule
extends Configured
implements FetchSchedule {
  private static final Log LOG = LogFactory.getLog(AbstractFetchSchedule.class);
  
  protected int defaultInterval;
  protected int maxInterval;
  
  private static final Set<HbaseColumn> COLUMNS = new HashSet<HbaseColumn>();
  
  static {
    COLUMNS.add(new HbaseColumn(WebTableColumns.FETCH_TIME));
    COLUMNS.add(new HbaseColumn(WebTableColumns.RETRIES));
    COLUMNS.add(new HbaseColumn(WebTableColumns.FETCH_INTERVAL));
  }
  
  public AbstractFetchSchedule() {
    super(null);
  }
  
  public AbstractFetchSchedule(Configuration conf) {
    super(conf);
  }
  
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) return;
    int oldDefaultInterval = conf.getInt("db.default.fetch.interval", 0);
    defaultInterval = conf.getInt("db.fetch.interval.default", 0);
    if (oldDefaultInterval > 0 && defaultInterval == 0) {
      defaultInterval = oldDefaultInterval * FetchSchedule.SECONDS_PER_DAY;
    }
    int oldMaxInterval = conf.getInt("db.max.fetch.interval", 0);
    maxInterval = conf.getInt("db.fetch.interval.max", 0 );
    if (oldMaxInterval > 0 && maxInterval == 0) { 
      maxInterval = oldMaxInterval * FetchSchedule.SECONDS_PER_DAY;
    }
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
   * @param row url's row
   */
  public void initializeSchedule(String url, WebTableRow row) {
    row.setFetchTime(System.currentTimeMillis());
    row.setFetchInterval(defaultInterval);
    row.setRetriesSinceFetch(0);
  }
  
  /**
   * Sets the <code>fetchInterval</code> and <code>fetchTime</code> on a
   * successfully fetched page. NOTE: this implementation resets the
   * retry counter - extending classes should call super.setFetchSchedule() to
   * preserve this behavior.
   */
  public void setFetchSchedule(String url, WebTableRow row,
          long prevFetchTime, long prevModifiedTime,
          long fetchTime, long modifiedTime, int state) {
    row.setRetriesSinceFetch(0);
  }
  
  /**
   * This method specifies how to schedule refetching of pages
   * marked as GONE. Default implementation increases fetchInterval by 50%,
   * and if it exceeds the <code>maxInterval</code> it calls
   * {@link #forceRefetch(Text, CrawlDatum, boolean)}.
   * @param url URL of the page
   * @param row url's row
   * @return adjusted page information, including all original information.
   * NOTE: this may be a different instance than {@param datum}, but
   * implementations should make sure that it contains at least all
   * information from {@param datum}.
   */
  public void setPageGoneSchedule(String url, WebTableRow row,
          long prevFetchTime, long prevModifiedTime, long fetchTime) {
    // no page is truly GONE ... just increase the interval by 50%
    // and try much later.
    int newFetchInterval = (int) (row.getFetchInterval() * 1.5f);
    row.setFetchInterval(newFetchInterval);
    row.setFetchTime(fetchTime + newFetchInterval * 1000L);
    if (maxInterval < newFetchInterval) forceRefetch(url, row, false);
  }
  
  /**
   * This method adjusts the fetch schedule if fetching needs to be
   * re-tried due to transient errors. The default implementation
   * sets the next fetch time 1 day in the future and increases
   * the retry counter.
   * @param url URL of the page
   * @param row url's row
   * @param prevFetchTime previous fetch time
   * @param prevModifiedTime previous modified time
   * @param fetchTime current fetch time
   */
  public void setPageRetrySchedule(String url, WebTableRow row,
          long prevFetchTime, long prevModifiedTime, long fetchTime) {
    row.setFetchTime(fetchTime + (long)FetchSchedule.SECONDS_PER_DAY);
    int oldRetries = row.getRetriesSinceFetch();
    row.setRetriesSinceFetch(oldRetries + 1);
  }
  
  /**
   * This method return the last fetch time of the CrawlDatum
   * @return the date as a long.
   */
  public long calculateLastFetchTime(WebTableRow row) {
    return row.getFetchTime() - row.getFetchInterval() * 1000L;
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
   * @param row url's row
   * @param curTime reference time (usually set to the time when the
   * fetchlist generation process was started).
   * @return true, if the page should be considered for inclusion in the current
   * fetchlist, otherwise false.
   */
  public boolean shouldFetch(String url, WebTableRow row, long curTime) {
    // pages are never truly GONE - we have to check them from time to time.
    // pages with too long fetchInterval are adjusted so that they fit within
    // maximum fetchInterval (segment retention period).
    long fetchTime = row.getFetchTime(); 
    if (fetchTime - curTime > maxInterval * 1000L) {
      row.setFetchInterval(Math.round(maxInterval * 0.9f));
      row.setFetchTime(curTime);
    }
    if (fetchTime > curTime) {
      return false;                                   // not time yet
    }
    return true;
  }
  
  /**
   * This method resets fetchTime, fetchInterval, modifiedTime,
   * retriesSinceFetch and page signature, so that it forces refetching.
   * @param url URL of the page
   * @param row url's row
   * @param asap if true, force refetch as soon as possible - this sets
   * the fetchTime to now. If false, force refetch whenever the next fetch
   * time is set.
   */
  public void forceRefetch(String url, WebTableRow row, boolean asap) {
    // reduce fetchInterval so that it fits within the max value
    if (row.getFetchInterval() > maxInterval)
      row.setFetchInterval(Math.round(maxInterval * 0.9f));
    row.setStatus(CrawlDatumHbase.STATUS_UNFETCHED);
    row.setRetriesSinceFetch(0);
    // TODO: row.setSignature(null) ??
    row.setModifiedTime(0L);
    if (asap) row.setFetchTime(System.currentTimeMillis());
  }
  

  public Set<HbaseColumn> getColumns() {
    return COLUMNS;
  }

}
