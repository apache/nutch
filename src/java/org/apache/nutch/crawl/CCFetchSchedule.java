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
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FetchSchedule which allows to reset the fetch interval to the default value
 */
public class CCFetchSchedule extends DefaultFetchSchedule {

  private final static Logger LOG = LoggerFactory
      .getLogger(CCFetchSchedule.class);

  private static final String RESET_FETCH_INTERVAL = "db.fetch.interval.reset";
  private static final String FETCH_TIME_MAX_DAYS_AHEAD = "db.fetch.time.max.days.ahead";
  private static final String RESET_NOT_MODIFIED = "db.fetch.reset.notmodified.after";

  private boolean resetFetchInterval = false;
  private boolean resetFetchTime = false;
  private long latestFetchTime;
  private long minModifiedTime = 0;

  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null)
      return;
    resetFetchInterval = conf.getBoolean(RESET_FETCH_INTERVAL, false);
    if (resetFetchInterval) {
      resetFetchInterval = true;
      LOG.info("Resetting fetch interval if exceeding {} = {}",
          "db.fetch.interval.max", maxInterval);
    }
    latestFetchTime = System.currentTimeMillis();
    int fetchTimeMaxDaysAhead = conf.getInt(FETCH_TIME_MAX_DAYS_AHEAD, 0);
    if (fetchTimeMaxDaysAhead > 0) {
      resetFetchTime = true;
      latestFetchTime += fetchTimeMaxDaysAhead * 24 * 60 * 60 * 1000;
      LOG.info("Resetting fetch time if more than {} = {} days ahead",
          FETCH_TIME_MAX_DAYS_AHEAD, fetchTimeMaxDaysAhead);
    }
    int secondsResetNotModifiedTime = conf.getInt(RESET_NOT_MODIFIED, 0);
    if (secondsResetNotModifiedTime > 0) {
      minModifiedTime = System.currentTimeMillis() - secondsResetNotModifiedTime * 1000;
    }
  }

  @Override
  public boolean shouldFetch(Text url, CrawlDatum datum, long curTime) {
    if (datum.getFetchTime() > curTime) {
      return false; // not time yet
    }
    if (datum.getModifiedTime() > 0
        && datum.getStatus() == CrawlDatum.STATUS_DB_NOTMODIFIED
        && datum.getModifiedTime() < minModifiedTime) {
      // trigger a full re-fetch of not-modified pages
      // (do not send if-not-modified-since requests)
      datum.setModifiedTime(0);
    }
    return true;
  }

  @Override
  public CrawlDatum setPageGoneSchedule(Text url, CrawlDatum datum,
      long prevFetchTime, long prevModifiedTime, long fetchTime) {
    if (resetFetchInterval && datum.getFetchInterval() > maxInterval) {
      datum.setFetchInterval(maxInterval);
    }
    return super.setPageGoneSchedule(url, datum, prevFetchTime,
        prevModifiedTime, fetchTime);
  }

  @Override
  public CrawlDatum setPageRetrySchedule(Text url, CrawlDatum datum,
      long prevFetchTime, long prevModifiedTime, long fetchTime) {
    if (resetFetchInterval && datum.getFetchInterval() > maxInterval) {
      datum.setFetchInterval(maxInterval);
    }
    return super.setPageRetrySchedule(url, datum, prevFetchTime,
        prevModifiedTime, fetchTime);
  }

  @Override
  public CrawlDatum setFetchSchedule(Text url, CrawlDatum datum,
          long prevFetchTime, long prevModifiedTime,
          long fetchTime, long modifiedTime, int state) {
    if (resetFetchInterval && datum.getFetchInterval() > maxInterval) {
      datum.setFetchInterval(maxInterval);
    }
    if (resetFetchTime && datum.getFetchTime() > latestFetchTime) {
      datum.setFetchTime(latestFetchTime);
    }
    return super.setFetchSchedule(url, datum, prevFetchTime, prevModifiedTime,
        fetchTime, modifiedTime, state);
  }
}
