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

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

import org.apache.nutch.crawl.CrawlDatum;

import static org.apache.nutch.crawl.CrawlDatum.*;

import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;

import static org.junit.Assert.*;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test transitions of {@link CrawlDatum} states during an update of
 * {@link CrawlDb} (command {@literal updatedb}):
 * <ul>
 * <li>simulate updatedb with the old CrawlDatum (db status) and the new one
 * (fetch status) and test whether the resulting CrawlDatum has the appropriate
 * status.</li>
 * <li>also check for further CrawlDatum fields (signature, etc.)</li>
 * <li>and additional conditions:</li>
 * <ul>
 * <li>retry counters</li>
 * <li>signatures</li>
 * <li>configuration properties</li>
 * <li>(additional) CrawlDatums of status linked (stemming from inlinks)</li>
 * </ul>
 * </li> </ul>
 */
public class TestCrawlDbStates {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestCrawlDbStates.class);

  protected static final byte[][] fetchDbStatusPairs = {
      { -1, STATUS_DB_UNFETCHED }, { STATUS_FETCH_SUCCESS, STATUS_DB_FETCHED },
      { STATUS_FETCH_GONE, STATUS_DB_GONE },
      { STATUS_FETCH_REDIR_TEMP, STATUS_DB_REDIR_TEMP },
      { STATUS_FETCH_REDIR_PERM, STATUS_DB_REDIR_PERM },
      { STATUS_FETCH_NOTMODIFIED, STATUS_DB_NOTMODIFIED },
      { STATUS_FETCH_RETRY, -1 }, // fetch_retry does not have a CrawlDb
                                  // counter-part
      { -1, STATUS_DB_DUPLICATE }, };

  /** tested {@link FetchSchedule} implementations */
  protected String[] schedules = { "DefaultFetchSchedule",
      "AdaptiveFetchSchedule" };

  /** CrawlDatum as result of a link */
  protected final CrawlDatum linked = new CrawlDatum(STATUS_LINKED,
      CrawlDBTestUtil.createConfiguration().getInt("db.fetch.interval.default",
          2592000), 0.1f);

  /**
   * Test the matrix of state transitions:
   * <ul>
   * <li>for all available {@link FetchSchedule} implementations</li>
   * <li>for every possible status in CrawlDb (including "not in CrawlDb")</li>
   * <li>for every possible fetch status</li>
   * <li>and zero or more (0-3) additional in-links</li>
   * </ul>
   * call {@literal updatedb} and check whether the resulting CrawlDb status is
   * the expected one.
   */
  @Test
  public void testCrawlDbStateTransitionMatrix() {
    LOG.info("Test CrawlDatum state transitions");
    Configuration conf = CrawlDBTestUtil.createConfiguration();
    CrawlDbUpdateUtil<CrawlDbReducer> updateDb = new CrawlDbUpdateUtil<CrawlDbReducer>(
        new CrawlDbReducer(), conf);
    int retryMax = conf.getInt("db.fetch.retry.max", 3);
    for (String sched : schedules) {
      LOG.info("Testing state transitions with " + sched);
      conf.set("db.fetch.schedule.class", "org.apache.nutch.crawl." + sched);
      FetchSchedule schedule = FetchScheduleFactory
          .getFetchSchedule(new JobConf(conf));
      for (int i = 0; i < fetchDbStatusPairs.length; i++) {
        byte fromDbStatus = fetchDbStatusPairs[i][1];
        for (int j = 0; j < fetchDbStatusPairs.length; j++) {
          byte fetchStatus = fetchDbStatusPairs[j][0];
          CrawlDatum fromDb = null;
          if (fromDbStatus == -1) {
            // nothing yet in CrawlDb
            // CrawlDatum added by FreeGenerator or via outlink
          } else {
            fromDb = new CrawlDatum();
            fromDb.setStatus(fromDbStatus);
            // initialize fetchInterval:
            schedule.initializeSchedule(CrawlDbUpdateUtil.dummyURL, fromDb);
          }
          // expected db status
          byte toDbStatus = fetchDbStatusPairs[j][1];
          if (fetchStatus == -1) {
            if (fromDbStatus == -1) {
              // nothing fetched yet: new document detected via outlink
              toDbStatus = STATUS_DB_UNFETCHED;
            } else {
              // nothing fetched but new inlinks detected: status is unchanged
              toDbStatus = fromDbStatus;
            }
          } else if (fetchStatus == STATUS_FETCH_RETRY) {
            // a simple test of fetch_retry (without retries)
            if (fromDb == null || fromDb.getRetriesSinceFetch() < retryMax) {
              toDbStatus = STATUS_DB_UNFETCHED;
            } else {
              toDbStatus = STATUS_DB_GONE;
            }
          }
          String fromDbStatusName = (fromDbStatus == -1 ? "<not in CrawlDb>"
              : getStatusName(fromDbStatus));
          String fetchStatusName = (fetchStatus == -1 ? "<only inlinks>"
              : CrawlDatum.getStatusName(fetchStatus));
          LOG.info(fromDbStatusName + " + " + fetchStatusName + " => "
              + getStatusName(toDbStatus));
          List<CrawlDatum> values = new ArrayList<CrawlDatum>();
          for (int l = 0; l <= 2; l++) { // number of additional in-links
            CrawlDatum fetch = null;
            if (fetchStatus == -1) {
              // nothing fetched, need at least one in-link
              if (l == 0)
                continue;
            } else {
              fetch = new CrawlDatum();
              if (fromDb != null) {
                fetch.set(fromDb);
              } else {
                // not yet in CrawlDb: added by FreeGenerator
                schedule.initializeSchedule(CrawlDbUpdateUtil.dummyURL, fetch);
              }
              fetch.setStatus(fetchStatus);
              fetch.setFetchTime(System.currentTimeMillis());
            }
            if (fromDb != null)
              values.add(fromDb);
            if (fetch != null)
              values.add(fetch);
            for (int n = 0; n < l; n++) {
              values.add(linked);
            }
            List<CrawlDatum> res = updateDb.update(values);
            if (res.size() != 1) {
              fail("CrawlDb update didn't result in one single CrawlDatum per URL");
              continue;
            }
            byte status = res.get(0).getStatus();
            if (status != toDbStatus) {
              fail("CrawlDb update for " + fromDbStatusName + " and "
                  + fetchStatusName + " and " + l + " inlinks results in "
                  + getStatusName(status) + " (expected: "
                  + getStatusName(toDbStatus) + ")");
            }
            values.clear();
          }
        }
      }
    }
  }

  /**
   * Test states after inject: inject must not modify the status of CrawlDatums
   * already in CrawlDb. Newly injected elements have status "db_unfetched".
   * Inject is simulated by calling {@link Injector.InjectReducer#reduce()}.
   */
  @Test
  public void testCrawlDbStatTransitionInject() {
    LOG.info("Test CrawlDatum states in Injector after inject");
    Configuration conf = CrawlDBTestUtil.createConfiguration();
    Injector.InjectReducer injector = new Injector.InjectReducer();
    CrawlDbUpdateTestDriver<Injector.InjectReducer> injectDriver =
        new CrawlDbUpdateTestDriver<Injector.InjectReducer>(injector, conf);
    ScoringFilters scfilters = new ScoringFilters(conf);
    for (String sched : schedules) {
      LOG.info("Testing inject with " + sched);
      conf.set("db.fetch.schedule.class", "org.apache.nutch.crawl." + sched);
      FetchSchedule schedule = FetchScheduleFactory
          .getFetchSchedule(new JobConf(conf));
      List<CrawlDatum> values = new ArrayList<CrawlDatum>();
      for (int i = 0; i < fetchDbStatusPairs.length; i++) {
        byte fromDbStatus = fetchDbStatusPairs[i][1];
        byte toDbStatus = fromDbStatus;
        if (fromDbStatus == -1) {
          toDbStatus = STATUS_DB_UNFETCHED;
        } else {
          CrawlDatum fromDb = new CrawlDatum();
          fromDb.setStatus(fromDbStatus);
          schedule.initializeSchedule(CrawlDbUpdateUtil.dummyURL, fromDb);
          values.add(fromDb);
        }
        LOG.info("inject "
            + (fromDbStatus == -1 ? "<not in CrawlDb>" : CrawlDatum
                .getStatusName(fromDbStatus)) + " + "
            + getStatusName(STATUS_INJECTED) + " => "
            + getStatusName(toDbStatus));
        CrawlDatum injected = new CrawlDatum(STATUS_INJECTED, conf.getInt(
            "db.fetch.interval.default", 2592000), 0.1f);
        schedule.initializeSchedule(CrawlDbUpdateUtil.dummyURL, injected);
        try {
          scfilters.injectedScore(CrawlDbUpdateUtil.dummyURL, injected);
        } catch (ScoringFilterException e) {
          LOG.error(StringUtils.stringifyException(e));
        }
        values.add(injected);
        List<CrawlDatum> res = injectDriver.update(values);
        if (res.size() != 1) {
          fail("Inject didn't result in one single CrawlDatum per URL");
          continue;
        }
        byte status = res.get(0).getStatus();
        if (status != toDbStatus) {
          fail("Inject for "
              + (fromDbStatus == -1 ? "" : getStatusName(fromDbStatus)
                  + " and ") + getStatusName(STATUS_INJECTED) + " results in "
              + getStatusName(status) + " (expected: "
              + getStatusName(toDbStatus) + ")");
        }
        values.clear();
      }
    }
  }

  /**
   * Test status db_notmodified detected by
   * <ul>
   * <li>signature comparison</li>
   * <li>or HTTP 304</li>
   * </ul>
   * In addition, test for all available {@link FetchSchedule} implementations
   * whether
   * <ul>
   * <li>modified time is set</li>
   * <li>re-fetch is triggered after a certain time to force the fetched content
   * to be in a recent segment (old segments are deleted, see comments in
   * {@link CrawlDbReducer#reduce(Text, Iterator, OutputCollector, Reporter)}</li>
   * </ul>
   */
  @Test
  public void testCrawlDbReducerNotModified() {
    LOG.info("Test state notmodified");
    Configuration conf = CrawlDBTestUtil.createConfiguration();
    // test not modified detected by signature comparison
    for (String sched : schedules) {
      String desc = "test notmodified by signature comparison + " + sched;
      LOG.info(desc);
      conf.set("db.fetch.schedule.class", "org.apache.nutch.crawl." + sched);
      ContinuousCrawlTestUtil crawlUtil = new CrawlTestFetchNotModified(conf);
      if (!crawlUtil.run(20)) {
        fail("failed: " + desc);
      }
    }
    // test not modified detected by HTTP 304
    for (String sched : schedules) {
      String desc = "test notmodified by HTTP 304 + " + sched;
      LOG.info(desc);
      conf.set("db.fetch.schedule.class", "org.apache.nutch.crawl." + sched);
      ContinuousCrawlTestUtil crawlUtil = new CrawlTestFetchNotModifiedHttp304(
          conf);
      if (!crawlUtil.run(20)) {
        fail("failed: " + desc);
      }
    }
  }

  protected class CrawlTestFetchNotModified extends ContinuousCrawlTestUtil {

    /** time of the current fetch */
    protected long currFetchTime;
    /** time the last fetch took place */
    protected long lastFetchTime;
    /**
     * time the document was fetched first (at all or after it has been changed)
     */
    protected long firstFetchTime;
    /** elapsed duration */
    protected long elapsedDuration = 0;
    /** state in CrawlDb before the last fetch */
    protected byte previousDbState;
    /** signature in CrawlDb of previous fetch */
    protected byte[] lastSignature;

    private long maxFetchInterval;
    private FetchSchedule schedule;

    CrawlTestFetchNotModified(Configuration conf) {
      super(conf);
      maxFetchInterval = conf.getLong("db.fetch.interval.max", 7776000); // default
                                                                         // = 90
                                                                         // days
      maxFetchInterval += (24 * 60 * 60); // but take one day more to avoid
                                          // false alarms
      maxFetchInterval *= 1000; // in milli-seconds
      schedule = FetchScheduleFactory.getFetchSchedule(new JobConf(conf));
    }

    @Override
    protected boolean check(CrawlDatum result) {
      if (lastFetchTime > 0
          && (currFetchTime - lastFetchTime) > maxFetchInterval) {
        LOG.error("last effective fetch (HTTP 200, not HTTP 304), at "
            + new Date(lastFetchTime)
            + ", took place more than db.fetch.interval.max time, "
            + "segment containing fetched content may have been deleted");
        return false;
      }
      switch (result.getStatus()) {
      case STATUS_DB_NOTMODIFIED:
        // db_notmodified is correct if the document has been fetched previously
        // and it has not been changed since
        if ((previousDbState == STATUS_DB_FETCHED || previousDbState == STATUS_DB_NOTMODIFIED)) {
          if (lastSignature != null
              && result.getSignature() != null
              && SignatureComparator._compare(lastSignature,
                  result.getSignature()) != 0) {
            LOG.error("document has changed (signature changed) but state is still "
                + getStatusName(STATUS_DB_NOTMODIFIED));
            return false;
          }
          LOG.info("ok: " + result);
          return checkModifiedTime(result, firstFetchTime);
        }
        LOG.warn("notmodified without previous fetch");
        break;
      case STATUS_DB_FETCHED:
        if (previousDbState == STATUS_DB_UNFETCHED) {
          LOG.info("ok (first fetch): " + result);
          return checkModifiedTime(result, firstFetchTime);
        } else if (lastSignature != null
            && result.getSignature() != null
            && SignatureComparator._compare(lastSignature,
                result.getSignature()) != 0) {
          LOG.info("ok (content changed): " + result);
          // expect modified time == now
          return checkModifiedTime(result, currFetchTime);
        } else {
          LOG.warn("document has not changed, db_notmodified expected");
        }
        break;
      case STATUS_DB_UNFETCHED:
        /**
         * Status db_unfetched is possible with {@link AdaptiveFetchSchedule}
         * because {@link CrawlDbReducer#reduce} calls
         * {@link FetchSchedule#forceRefetch} to force a re-fetch if fetch
         * interval grows too large.
         */
        if (schedule.getClass() == AdaptiveFetchSchedule.class) {
          LOG.info("state set to unfetched by AdaptiveFetchSchedule");
          if (result.getSignature() != null) {
            LOG.warn("must reset signature: " + result);
            return false;
          }
          LOG.info("ok: " + result);
          firstFetchTime = 0;
          return true;
        }
      }
      LOG.warn("wrong result: " + result);
      return false;
    }

    // test modified time
    private boolean checkModifiedTime(CrawlDatum result, long modifiedTime) {
      if (modifiedTime == result.getModifiedTime()) {
        return true;
      }
      LOG.error("wrong modified time: " + new Date(result.getModifiedTime())
          + " (expected " + new Date(modifiedTime) + ")");
      return false;
    }

    @Override
    protected CrawlDatum fetch(CrawlDatum datum, long currentTime) {
      lastFetchTime = currFetchTime;
      currFetchTime = currentTime;
      if (lastFetchTime > 0)
        elapsedDuration += (currFetchTime - lastFetchTime);
      previousDbState = datum.getStatus();
      lastSignature = datum.getSignature();
      datum = super.fetch(datum, currentTime);
      if (firstFetchTime == 0) {
        firstFetchTime = currFetchTime;
      } else if (elapsedDuration < (duration / 2)) {
        // simulate frequent modifications in the first "year"
        changeContent();
        firstFetchTime = currFetchTime;
      }
      return datum;
    }
  }

  protected class CrawlTestFetchNotModifiedHttp304 extends
      CrawlTestFetchNotModified {

    CrawlTestFetchNotModifiedHttp304(Configuration conf) {
      super(conf);
    }

    @Override
    protected CrawlDatum fetch(CrawlDatum datum, long currentTime) {
      lastFetchTime = currFetchTime;
      currFetchTime = currentTime;
      previousDbState = datum.getStatus();
      lastSignature = datum.getSignature();
      int httpCode;
      /*
       * document is "really" fetched (no HTTP 304) - if last-modified time or
       * signature are unset (page has not been fetched before or fetch is
       * forced) - for test purposes, we simulate a modified after "one year"
       */
      if (datum.getModifiedTime() == 0 && datum.getSignature() == null
          || (currFetchTime - firstFetchTime) > (duration / 2)) {
        firstFetchTime = currFetchTime;
        httpCode = 200;
        datum.setStatus(STATUS_FETCH_SUCCESS);
        // modify content to change signature
        changeContent();
      } else {
        httpCode = 304;
        datum.setStatus(STATUS_FETCH_NOTMODIFIED);
      }
      LOG.info("fetched with HTTP " + httpCode + " => "
          + getStatusName(datum.getStatus()));
      datum.setFetchTime(currentTime);
      return datum;
    }
  }

  /**
   * NUTCH-1245: a fetch_gone should always result in a db_gone.
   * <p>
   * Even in a long-running continuous crawl, when a gone page is re-fetched
   * several times over time.
   * </p>
   */
  @Test
  public void testCrawlDbReducerPageGoneSchedule1() {
    LOG.info("NUTCH-1245: test long running continuous crawl");
    ContinuousCrawlTestUtil crawlUtil = new ContinuousCrawlTestUtil(
        STATUS_FETCH_GONE, STATUS_DB_GONE);
    if (!crawlUtil.run(20)) {
      fail("fetch_gone did not result in a db_gone (NUTCH-1245)");
    }
  }

  /**
   * NUTCH-1245: a fetch_gone should always result in a db_gone.
   * <p>
   * As some kind of misconfiguration set db.fetch.interval.default to a value
   * &gt; (fetchIntervalMax * 1.5).
   * </p>
   */
  @Test
  public void testCrawlDbReducerPageGoneSchedule2() {
    LOG.info("NUTCH-1245 (misconfiguration): test with db.fetch.interval.default > (1.5 * db.fetch.interval.max)");
    Configuration conf = CrawlDBTestUtil.createConfiguration();
    int fetchIntervalMax = conf.getInt("db.fetch.interval.max", 0);
    conf.setInt("db.fetch.interval.default", 3 + (int) (fetchIntervalMax * 1.5));
    ContinuousCrawlTestUtil crawlUtil = new ContinuousCrawlTestUtil(conf,
        STATUS_FETCH_GONE, STATUS_DB_GONE);
    if (!crawlUtil.run(0)) {
      fail("fetch_gone did not result in a db_gone (NUTCH-1245)");
    }
  }

  /**
   * Test whether signatures are reset for "content-less" states (gone,
   * redirect, etc.): otherwise, if this state is temporary and the document
   * appears again with the old content, it may get marked as not_modified in
   * CrawlDb just after the redirect state. In this case we cannot expect
   * content in segments. Cf. NUTCH-1422: reset signature for redirects.
   */
  // TODO: can only test if solution is done in CrawlDbReducer
  @Test
  public void testSignatureReset() {
    LOG.info("NUTCH-1422 must reset signature for redirects and similar states");
    Configuration conf = CrawlDBTestUtil.createConfiguration();
    for (String sched : schedules) {
      LOG.info("Testing reset signature with " + sched);
      conf.set("db.fetch.schedule.class", "org.apache.nutch.crawl." + sched);
      ContinuousCrawlTestUtil crawlUtil = new CrawlTestSignatureReset(conf);
      if (!crawlUtil.run(20)) {
        fail("failed: signature not reset");
      }
    }
  }

  private class CrawlTestSignatureReset extends ContinuousCrawlTestUtil {

    byte[][] noContentStates = { { STATUS_FETCH_GONE, STATUS_DB_GONE },
        { STATUS_FETCH_REDIR_TEMP, STATUS_DB_REDIR_TEMP },
        { STATUS_FETCH_REDIR_PERM, STATUS_DB_REDIR_PERM } };

    int counter = 0;
    byte fetchState;

    public CrawlTestSignatureReset(Configuration conf) {
      super(conf);
    }

    @Override
    protected CrawlDatum fetch(CrawlDatum datum, long currentTime) {
      datum = super.fetch(datum, currentTime);
      counter++;
      // flip-flopping between successful fetch and one of content-less states
      if (counter % 2 == 1) {
        fetchState = STATUS_FETCH_SUCCESS;
      } else {
        fetchState = noContentStates[(counter % 6) / 2][0];
      }
      LOG.info("Step " + counter + ": fetched with "
          + getStatusName(fetchState));
      datum.setStatus(fetchState);
      return datum;
    }

    @Override
    protected boolean check(CrawlDatum result) {
      if (result.getStatus() == STATUS_DB_NOTMODIFIED
          && !(fetchState == STATUS_FETCH_SUCCESS || fetchState == STATUS_FETCH_NOTMODIFIED)) {
        LOG.error("Should never get into state "
            + getStatusName(STATUS_DB_NOTMODIFIED) + " from "
            + getStatusName(fetchState));
        return false;
      }
      if (result.getSignature() != null
          && !(result.getStatus() == STATUS_DB_FETCHED || result.getStatus() == STATUS_DB_NOTMODIFIED)) {
        LOG.error("Signature not reset in state "
            + getStatusName(result.getStatus()));
        // ok here: since it's not the problem itself (the db_notmodified), but
        // the reason for it
      }
      return true;
    }

  }

}
