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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for AdaptiveFetchSchedule.
 * 
 */
public class TestAdaptiveFetchSchedule extends TestCase {

  private float inc_rate;
  private float dec_rate;
  private Configuration conf;
  private long curTime, lastModified;
  private int changed, interval, calculateInterval;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
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
      assertEquals(getInterval, interval);

    } else if (changed == FetchSchedule.STATUS_MODIFIED) {
      calculateInterval = (int) (interval - (interval * dec_rate));
      assertEquals(getInterval, calculateInterval);

    } else if (changed == FetchSchedule.STATUS_NOTMODIFIED) {
      calculateInterval = (int) (interval + (interval * inc_rate));
      assertEquals(getInterval, calculateInterval);
    }

  }

}
