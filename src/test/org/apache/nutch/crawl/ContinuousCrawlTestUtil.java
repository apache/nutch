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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.crawl.CrawlDbUpdateUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emulate a continuous crawl for one URL.
 * 
 */
public class ContinuousCrawlTestUtil extends TestCase {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  protected static Text dummyURL = new Text("http://nutch.apache.org/");

  protected static Reducer<Text, CrawlDatum, Text, CrawlDatum>.Context defaultContext = CrawlDBTestUtil.createContext();

  protected long interval = FetchSchedule.SECONDS_PER_DAY * 1000; // (default)
                                                                  // launch
                                                                  // crawler
                                                                  // every day
  protected long duration = 2 * 365L * FetchSchedule.SECONDS_PER_DAY * 1000L; // run
                                                                              // for
                                                                              // two
                                                                              // years

  protected Reducer<Text, CrawlDatum, Text, CrawlDatum>.Context context;
  private FetchSchedule schedule;

  /** status a fetched datum should get */
  protected byte fetchStatus = CrawlDatum.STATUS_FETCH_SUCCESS;
  /** expected status of the resulting Db datum */
  protected byte expectedDbStatus = CrawlDatum.STATUS_DB_FETCHED;

  /** for signature calculation */
  protected Signature signatureImpl;
  protected Content content = new Content();

  {
    byte[] data = { 'n', 'u', 't', 'c', 'h' };
    content.setContent(data);
  }

  protected ContinuousCrawlTestUtil(Reducer<Text, CrawlDatum, Text, CrawlDatum>.Context cont) {
    context = cont;
    Configuration conf = context.getConfiguration();
    schedule = FetchScheduleFactory.getFetchSchedule(conf);
    signatureImpl = SignatureFactory.getSignature(conf);
  }

  protected ContinuousCrawlTestUtil(Reducer<Text, CrawlDatum, Text, CrawlDatum>.Context cont, byte fetchStatus,
      byte expectedDbStatus) {
    this(cont);
    this.fetchStatus = fetchStatus;
    this.expectedDbStatus = expectedDbStatus;
  }

  protected ContinuousCrawlTestUtil() {
    this(defaultContext);
  }

  protected ContinuousCrawlTestUtil(byte fetchStatus, byte expectedDbStatus) {
    this(defaultContext, fetchStatus, expectedDbStatus);
  }

  /** set the interval the crawl is relaunched (default: every day) */
  protected void setInterval(int seconds) {
    interval = seconds * 1000L;
  }

  /** set the duration of the continuous crawl (default = 2 years) */
  protected void setDuraction(int seconds) {
    duration = seconds * 1000L;
  }

  /**
   * default fetch action: set status and time
   * 
   * @param datum
   *          CrawlDatum to fetch
   * @param currentTime
   *          current time used to set the fetch time via
   *          {@link CrawlDatum#setFetchTime(long)}
   * @return the modified CrawlDatum
   */
  protected CrawlDatum fetch(CrawlDatum datum, long currentTime) {
    datum.setStatus(fetchStatus);
    datum.setFetchTime(currentTime);
    return datum;
  }

  /**
   * get signature for content and configured signature implementation
   */
  protected byte[] getSignature() {
    return signatureImpl.calculate(content, null);
  }

  /**
   * change content to force a changed signature
   */
  protected void changeContent() {
    byte[] data = Arrays.copyOf(content.getContent(),
        content.getContent().length + 1);
    data[content.getContent().length] = '2'; // append one byte
    content.setContent(data);
    LOG.info("document content changed");
  }

  /**
   * default parse action: add signature if successfully fetched
   * 
   * @param fetchDatum
   *          fetch datum
   * @return list of all datums resulting from parse (status: signature, linked,
   *         parse_metadata)
   */
  protected List<CrawlDatum> parse(CrawlDatum fetchDatum) {
    List<CrawlDatum> parseDatums = new ArrayList<CrawlDatum>(0);
    if (fetchDatum.getStatus() == CrawlDatum.STATUS_FETCH_SUCCESS) {
      CrawlDatum signatureDatum = new CrawlDatum(CrawlDatum.STATUS_SIGNATURE, 0);
      signatureDatum.setSignature(getSignature());
      parseDatums.add(signatureDatum);
    }
    return parseDatums;
  }

  /**
   * default implementation to check the result state
   * 
   * @param datum
   *          the CrawlDatum to be checked
   * @return true if the check succeeds
   */
  protected boolean check(CrawlDatum datum) {
    if (datum.getStatus() != expectedDbStatus)
      return false;
    return true;
  }

  /**
   * Run the continuous crawl.
   * <p>
   * A loop emulates a continuous crawl launched in regular intervals (see
   * {@link #setInterval(int)} over a longer period ({@link #setDuraction(int)}.
   * 
   * <ul>
   * <li>every "round" emulates
   * <ul>
   * <li>a fetch (see {@link #fetch(CrawlDatum, long)})</li>
   * <li>{@literal updatedb} which returns a {@link CrawlDatum}</li>
   * </ul>
   * <li>the returned CrawlDatum is used as input for the next round</li>
   * <li>and is checked whether it is correct (see {@link #check(CrawlDatum)})
   * </ul>
   * </p>
   * 
   * @param maxErrors
   *          (if > 0) continue crawl even if the checked CrawlDatum is not
   *          correct, but stop after max. number of errors
   * 
   * @return false if a check of CrawlDatum failed, true otherwise
   * @throws IOException 
   */
  protected boolean run(int maxErrors) throws IOException {

    long now = System.currentTimeMillis();

    CrawlDbUpdateUtil<CrawlDbReducer> updateDb = new CrawlDbUpdateUtil<CrawlDbReducer>(
        new CrawlDbReducer(), context);

    /* start with a db_unfetched */
    CrawlDatum dbDatum = new CrawlDatum();
    dbDatum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
    schedule.initializeSchedule(dummyURL, dbDatum); // initialize fetchInterval
    dbDatum.setFetchTime(now);

    LOG.info("Emulate a continuous crawl, launched every "
        + (interval / (FetchSchedule.SECONDS_PER_DAY * 1000)) + " day ("
        + (interval / 1000) + " seconds)");
    long maxTime = (now + duration);
    long nextTime = now;
    long lastFetchTime = -1;
    boolean ok = true; // record failure but keep going
    CrawlDatum fetchDatum = new CrawlDatum();
    /*
     * Keep copies because CrawlDbReducer.reduce() and
     * FetchSchedule.shouldFetch() may alter the references. Copies are used for
     * verbose logging in case of an error.
     */
    CrawlDatum copyDbDatum = new CrawlDatum();
    CrawlDatum copyFetchDatum = new CrawlDatum();
    CrawlDatum afterShouldFetch = new CrawlDatum();
    int errorCount = 0;
    while (nextTime < maxTime) {
      LOG.info("check: " + new Date(nextTime));
      fetchDatum.set(dbDatum);
      copyDbDatum.set(dbDatum);
      if (schedule.shouldFetch(dummyURL, fetchDatum, nextTime)) {
        LOG.info("... fetching now (" + new Date(nextTime) + ")");
        if (lastFetchTime > -1) {
          LOG.info("(last fetch: " + new Date(lastFetchTime) + " = "
              + TimingUtil.elapsedTime(lastFetchTime, nextTime) + " ago)");
        }
        lastFetchTime = nextTime;
        afterShouldFetch.set(fetchDatum);
        fetchDatum = fetch(fetchDatum, nextTime);
        copyFetchDatum.set(fetchDatum);
        List<CrawlDatum> values = new ArrayList<CrawlDatum>();
        values.add(dbDatum);
        values.add(fetchDatum);
        values.addAll(parse(fetchDatum));
        List<CrawlDatum> res = updateDb.update(values);
        assertNotNull("null returned", res);
        assertFalse("no CrawlDatum", 0 == res.size());
        assertEquals("more than one CrawlDatum", 1, res.size());
        if (!check(res.get(0))) {
          LOG.info("previously in CrawlDb: " + copyDbDatum);
          LOG.info("after shouldFetch(): " + afterShouldFetch);
          LOG.info("fetch: " + fetchDatum);
          LOG.warn("wrong result in CrawlDb: " + res.get(0));
          if (++errorCount >= maxErrors) {
            if (maxErrors > 0) {
              LOG.error("Max. number of errors " + maxErrors
                  + " reached. Stopping.");
            }
            return false;
          } else {
            ok = false; // record failure but keep going
          }
        }
        /* use the returned CrawlDatum for the next fetch */
        dbDatum = res.get(0);
      }
      nextTime += interval;
    }
    return ok;
  }

}
