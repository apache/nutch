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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * This class provides common methods for implementations of
 * <code>FetchSchedule</code>.
 * 
 * @author Andrzej Bialecki
 */
public abstract class AbstractFetchSchedule extends Configured implements
    FetchSchedule {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  protected int defaultInterval;
  protected int maxInterval;

  public AbstractFetchSchedule() {
    super(null);
  }

  public AbstractFetchSchedule(Configuration conf) {
    super(conf);
  }

  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null)
      return;
    defaultInterval = conf.getInt("db.fetch.interval.default", 0);
    maxInterval = conf.getInt("db.fetch.interval.max", 0);
    LOG.info("defaultInterval=" + defaultInterval);
    LOG.info("maxInterval=" + maxInterval);
  }

  /**
   * Initialize fetch schedule related data. Implementations should at least set
   * the <code>fetchTime</code> and <code>fetchInterval</code>. The default
   * implementation sets the <code>fetchTime</code> to now, using the default
   * <code>fetchInterval</code>.
   * 
   * @param url
   *          URL of the page.
   * 
   * @param datum
   *          datum instance to be initialized (modified in place).
   */
  public CrawlDatum initializeSchedule(Text url, CrawlDatum datum) {
    datum.setFetchTime(System.currentTimeMillis());
    datum.setFetchInterval(defaultInterval);
    datum.setRetriesSinceFetch(0);
    return datum;
  }

  /**
   * Sets the <code>fetchInterval</code> and <code>fetchTime</code> on a
   * successfully fetched page. NOTE: this implementation resets the retry
   * counter - extending classes should call super.setFetchSchedule() to
   * preserve this behavior.
   */
  public CrawlDatum setFetchSchedule(Text url, CrawlDatum datum,
      long prevFetchTime, long prevModifiedTime, long fetchTime,
      long modifiedTime, int state) {
    datum.setRetriesSinceFetch(0);
    return datum;
  }

  /**
   * This method specifies how to schedule refetching of pages marked as GONE.
   * Default implementation increases fetchInterval by 50% but the value may
   * never exceed <code>maxInterval</code>.
   * 
   * @param url
   *          URL of the page.
   * 
   * @param datum
   *          datum instance to be adjusted.
   * 
   * @return adjusted page information, including all original information.
   *         NOTE: this may be a different instance than @see CrawlDatum, but
   *         implementations should make sure that it contains at least all
   *         information from @see CrawlDatum.
   */
  public CrawlDatum setPageGoneSchedule(Text url, CrawlDatum datum,
      long prevFetchTime, long prevModifiedTime, long fetchTime) {
    // no page is truly GONE ... just increase the interval by 50%
    // and try much later.
    if ((datum.getFetchInterval() * 1.5f) < maxInterval)
      datum.setFetchInterval(datum.getFetchInterval() * 1.5f);
    else
      datum.setFetchInterval(maxInterval * 0.9f);
    datum.setFetchTime(fetchTime + (long) datum.getFetchInterval() * 1000);
    return datum;
  }

  /**
   * This method adjusts the fetch schedule if fetching needs to be re-tried due
   * to transient errors. The default implementation sets the next fetch time 1
   * day in the future and increases the retry counter.
   * 
   * @param url
   *          URL of the page.
   * 
   * @param datum
   *          page information.
   * 
   * @param prevFetchTime
   *          previous fetch time.
   * 
   * @param prevModifiedTime
   *          previous modified time.
   * 
   * @param fetchTime
   *          current fetch time.
   * 
   * @return adjusted page information, including all original information.
   *         NOTE: this may be a different instance than @see CrawlDatum, but
   *         implementations should make sure that it contains at least all
   *         information from @see CrawlDatum.
   */
  public CrawlDatum setPageRetrySchedule(Text url, CrawlDatum datum,
      long prevFetchTime, long prevModifiedTime, long fetchTime) {
    datum.setFetchTime(fetchTime + (long) SECONDS_PER_DAY * 1000);
    datum.setRetriesSinceFetch(datum.getRetriesSinceFetch() + 1);
    return datum;
  }

  /**
   * This method return the last fetch time of the CrawlDatum
   * 
   * @return the date as a long.
   */
  public long calculateLastFetchTime(CrawlDatum datum) {
    if (datum.getStatus() == CrawlDatum.STATUS_DB_UNFETCHED) {
      return 0L;
    } else {
      return datum.getFetchTime() - (long) datum.getFetchInterval() * 1000;
    }
  }

  /**
   * This method provides information whether the page is suitable for selection
   * in the current fetchlist. NOTE: a true return value does not guarantee that
   * the page will be fetched, it just allows it to be included in the further
   * selection process based on scores. The default implementation checks
   * <code>fetchTime</code>, if it is higher than the <code>curTime</code> it
   * returns false, and true otherwise. It will also check that fetchTime is not
   * too remote (more than <code>maxInterval</code>, in which case it lowers the
   * interval and returns true.
   * 
   * @param url
   *          URL of the page.
   * 
   * @param datum
   *          datum instance.
   * 
   * @param curTime
   *          reference time (usually set to the time when the fetchlist
   *          generation process was started).
   * 
   * @return true, if the page should be considered for inclusion in the current
   *         fetchlist, otherwise false.
   */
  public boolean shouldFetch(Text url, CrawlDatum datum, long curTime) {
    // pages are never truly GONE - we have to check them from time to time.
    // pages with too long a fetchInterval are adjusted so that they fit within
    // a maximum fetchInterval (segment retention period).
    if (datum.getFetchTime() - curTime > (long) maxInterval * 1000) {
      if (datum.getFetchInterval() > maxInterval) {
        datum.setFetchInterval(maxInterval * 0.9f);
      }
      datum.setFetchTime(curTime);
    }
    if (datum.getFetchTime() > curTime) {
      return false; // not time yet
    }
    return true;
  }

  /**
   * This method resets fetchTime, fetchInterval, modifiedTime,
   * retriesSinceFetch and page signature, so that it forces refetching.
   * 
   * @param url
   *          URL of the page.
   * 
   * @param datum
   *          datum instance.
   * 
   * @param asap
   *          if true, force refetch as soon as possible - this sets the
   *          fetchTime to now. If false, force refetch whenever the next fetch
   *          time is set.
   */
  public CrawlDatum forceRefetch(Text url, CrawlDatum datum, boolean asap) {
    // reduce fetchInterval so that it fits within the max value
    if (datum.getFetchInterval() > maxInterval)
      datum.setFetchInterval(maxInterval * 0.9f);
    datum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
    datum.setRetriesSinceFetch(0);
    datum.setSignature(null);
    datum.setModifiedTime(0L);
    if (asap)
      datum.setFetchTime(System.currentTimeMillis());
    return datum;
  }

}
