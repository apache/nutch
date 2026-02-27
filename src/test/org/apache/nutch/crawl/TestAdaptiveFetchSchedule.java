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
package org.apache.nutch.crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;

/**
 * Test cases for AdaptiveFetchSchedule.
 * 
 */
public class TestAdaptiveFetchSchedule {

  private float inc_rate;
  private float dec_rate;
  private Configuration conf;
  private long curTime, lastModified;
  private int changed, interval, calculateInterval;

  @BeforeEach
  public void setUp() throws Exception {
    conf = NutchConfiguration.create();
    inc_rate = conf.getFloat("db.fetch.schedule.adaptive.inc_rate", 0.2f);
    dec_rate = conf.getFloat("db.fetch.schedule.adaptive.dec_rate", 0.2f);
    interval = 100;
    lastModified = 0;
  }

  /**
   * Test the core functionality of AdaptiveFetchSchedule.
   * 
   */

  @Test
  public void testAdaptiveFetchSchedule() {

    FetchSchedule fs = new AdaptiveFetchSchedule();
    fs.setConf(conf);

    CrawlDatum p = prepareCrawlDatum();
    Text url = new Text("http://www.example.com");

    changed = FetchSchedule.STATUS_UNKNOWN;
    fs.setFetchSchedule(url, p, p.getFetchTime(), p.getModifiedTime(), curTime,
        lastModified, changed);
    validateFetchInterval(changed, p.getFetchInterval());

    changed = FetchSchedule.STATUS_MODIFIED;
    fs.setFetchSchedule(url, p, p.getFetchTime(), p.getModifiedTime(), curTime,
        lastModified, changed);
    validateFetchInterval(changed, p.getFetchInterval());
    p.setFetchInterval(interval);

    changed = FetchSchedule.STATUS_NOTMODIFIED;
    fs.setFetchSchedule(url, p, p.getFetchTime(), p.getModifiedTime(), curTime,
        lastModified, changed);
    validateFetchInterval(changed, p.getFetchInterval());

  }

  /**
   * Prepare a CrawlDatum (STATUS_DB_UNFETCHED) to Test AdaptiveFetchSchedule.
   * 
   * @return properly initialized CrawlDatum
   */
  public CrawlDatum prepareCrawlDatum() {
    CrawlDatum p = new CrawlDatum();
    p.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
    p.setFetchInterval(interval);
    p.setScore(1.0f);
    p.setFetchTime(0);
    return p;
  }

  /**
   * 
   * The Method validates interval values according to changed parameter.
   * 
   * @param changed
   *          status value to check calculated interval value.
   * @param getInterval
   *          to test IntervalValue from CrawlDatum which is calculated via
   *          AdaptiveFetchSchedule algorithm.
   */
  private void validateFetchInterval(int changed, int getInterval) {

    if (changed == FetchSchedule.STATUS_UNKNOWN) {
      assertThat(interval, is(getInterval));

    } else if (changed == FetchSchedule.STATUS_MODIFIED) {
      calculateInterval = (int) (interval - (interval * dec_rate));
      assertThat(calculateInterval, is(getInterval));

    } else if (changed == FetchSchedule.STATUS_NOTMODIFIED) {
      calculateInterval = (int) (interval + (interval * inc_rate));
      assertThat(calculateInterval, is(getInterval));
    }

  }
  
  /**
   * Test https://issues.apache.org/jira/browse/NUTCH-1564
   */
  @Test
  public void testSetFetchSchedule1() {
    // db.fetch.schedule.adaptive.sync_delta_rate = 0.3 (default)
    // db.fetch.interval.default               = 172800 (2 days)
    // db.fetch.schedule.adaptive.min_interval =  86400 (1 day)
    // db.fetch.schedule.adaptive.max_interval = 604800 (7 days)
    // db.fetch.interval.max                   = 604800 (7 days)
    // 3-days cycle
    // 30 days since last modified
    doTestSetFetchSchedule(0.3, 2, 1, 7, 7, 3, 30);
  }

  @Test
  public void testSetFetchSchedule2() {
    // db.fetch.schedule.adaptive.sync_delta_rate = 0.3 (default)
    // db.fetch.interval.default               = 86400 (1 day)
    // db.fetch.schedule.adaptive.min_interval =  86400 (1 day)
    // db.fetch.schedule.adaptive.max_interval = 172800 (2 days)
    // db.fetch.interval.max                   = 604800 (7 days)
    // 1-day cycle
    // 10 days since last modified
    doTestSetFetchSchedule(0.3, 1, 1, 2, 7, 1, 10);
  }

  @Test
  public void testSetFetchSchedule3() {
    // db.fetch.schedule.adaptive.sync_delta_rate = 0.3 (default)
    // db.fetch.interval.default               = 172800 (2 days)
    // db.fetch.schedule.adaptive.min_interval =  86400 (1 day)
    // db.fetch.schedule.adaptive.max_interval = 864000 (10 days)
    // db.fetch.interval.max                   = 864000 (10 days)
    // 3-days cycle
    // 180 days since last modified
    doTestSetFetchSchedule(0.3, 2, 1, 10, 10, 3, 180);
  }

  private void doTestSetFetchSchedule(double deltaRate, int intervalDefaultDays, 
      int minIntervalDays, int maxIntervalDays, int intervalMaxDays,
      int previousFetchTimeDays, int modifiedTimeDays) {
    // need to properly override defaults
    Properties props = new Properties();
    props.setProperty("db.fetch.schedule.class", "org.apache.nutch.crawl.AdaptiveFetchSchedule");
    props.setProperty("db.fetch.schedule.adaptive.sync_delta", "true"); // default
    props.setProperty("db.fetch.schedule.adaptive.sync_delta_rate", String.valueOf(deltaRate));
    props.setProperty("db.fetch.interval.default", String.valueOf(FetchSchedule.SECONDS_PER_DAY * intervalDefaultDays));
    props.setProperty("db.fetch.schedule.adaptive.min_interval", String.valueOf(FetchSchedule.SECONDS_PER_DAY * minIntervalDays));
    props.setProperty("db.fetch.schedule.adaptive.max_interval", String.valueOf(FetchSchedule.SECONDS_PER_DAY * maxIntervalDays));
    props.setProperty("db.fetch.interval.max", String.valueOf(FetchSchedule.SECONDS_PER_DAY * intervalMaxDays));
    
    conf = NutchConfiguration.create(true, props);
    inc_rate = conf.getFloat("db.fetch.schedule.adaptive.inc_rate", 0.2f); // default
    dec_rate = conf.getFloat("db.fetch.schedule.adaptive.dec_rate", 0.2f); // default

    // ignore adaptive-host-specific-intervals.txt
    Text url = new Text("http://www.example2.com");
    
    AdaptiveFetchSchedule fs = new AdaptiveFetchSchedule();
    fs.setConf(conf);
    
    CrawlDatum datum = prepareCrawlDatum();
    Date fetchTime = Date.from(Instant.now());
    Date previousFetchTime = Date.from(Instant.now().minus(Duration.ofDays(previousFetchTimeDays)));
    Date modifiedTime = Date.from(Instant.now().minus(Duration.ofDays(modifiedTimeDays)));
    datum.setStatus(CrawlDatum.STATUS_FETCH_SUCCESS);
    datum.setRetriesSinceFetch(0);
    datum.setModifiedTime(modifiedTime.getTime());
    datum.setFetchTime(fetchTime.getTime()); 
    
    System.out.println("CrawlDatum fetchTime: " +  fetchTime + "; modifiedTime: " + modifiedTime);
    
    fs.setFetchSchedule(url, datum, previousFetchTime.getTime(), modifiedTime.getTime(), 
        fetchTime.getTime(), modifiedTime.getTime(), CrawlDatum.STATUS_DB_NOTMODIFIED);
    
    Date nextFetchTime = new Date(datum.getFetchTime());
    System.out.println("CrawlDatum next fetchTime: " + nextFetchTime);
    
    assertTrue(nextFetchTime.after(fetchTime));
    // adapt milliseconds to seconds
    long fetchTimeDiff = (nextFetchTime.getTime() - fetchTime.getTime()) / 1000L ;
    assertTrue(fetchTimeDiff >= FetchSchedule.SECONDS_PER_DAY * minIntervalDays);
    assertTrue(fetchTimeDiff <= FetchSchedule.SECONDS_PER_DAY * maxIntervalDays);
  }

}
