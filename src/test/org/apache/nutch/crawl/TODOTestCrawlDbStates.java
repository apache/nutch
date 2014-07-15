package org.apache.nutch.crawl;

import static org.apache.nutch.crawl.CrawlDatum.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.TimingUtil;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TODOTestCrawlDbStates extends TestCrawlDbStates {

  private static final Logger LOG = LoggerFactory.getLogger(TODOTestCrawlDbStates.class);

  /**
   * NUTCH-578: a fetch_retry should result in a db_gone if db.fetch.retry.max is reached.
   * Retry counter has to be reset appropriately.
   */
  @Test
  public void testCrawlDbReducerPageRetrySchedule() {
    LOG.info("NUTCH-578: test long running continuous crawl with fetch_retry");
    ContinuousCrawlTestUtil crawlUtil = new ContinuousCrawlTestFetchRetry();
    // keep going for long, to "provoke" a retry counter overflow
    if (!crawlUtil.run(150)) {
      fail("fetch_retry did not result in a db_gone if retry counter > maxRetries (NUTCH-578)");
    }
  }

  private class ContinuousCrawlTestFetchRetry extends ContinuousCrawlTestUtil {

    private int retryMax = 3;
    private int totalRetries = 0;

    ContinuousCrawlTestFetchRetry() {
      super();
      fetchStatus = STATUS_FETCH_RETRY;
      retryMax = configuration.getInt("db.fetch.retry.max", retryMax);
    }

    @Override
    protected CrawlDatum fetch(CrawlDatum datum, long currentTime) {
      datum.setStatus(fetchStatus);
      datum.setFetchTime(currentTime);
      totalRetries++;
      return datum;
    }

    @Override
    protected boolean check(CrawlDatum result) {
      if (result.getRetriesSinceFetch() > retryMax) {
        LOG.warn("Retry counter > db.fetch.retry.max: " + result);
      } else if (result.getRetriesSinceFetch() == Byte.MAX_VALUE) {
        LOG.warn("Retry counter max. value reached (overflow imminent): "
            + result);
      } else if (result.getRetriesSinceFetch() < 0) {
        LOG.error("Retry counter overflow: " + result);
        return false;
      }
      // use retry counter bound to this class (totalRetries)
      // instead of result.getRetriesSinceFetch() because the retry counter
      // in CrawlDatum could be reset (eg. NUTCH-578_v5.patch)
      if (totalRetries < retryMax) {
        if (result.getStatus() == STATUS_DB_UNFETCHED) {
          LOG.info("ok: " + result);
          result.getRetriesSinceFetch();
          return true;
        }
      } else {
        if (result.getStatus() == STATUS_DB_GONE) {
          LOG.info("ok: " + result);
          return true;
        }
      }
      LOG.warn("wrong: " + result);
      return false;
    }

  }

  /**
   * NUTCH-1564 AdaptiveFetchSchedule: sync_delta forces immediate re-fetch for
   * documents not modified
   * <p>
   * Problem: documents not modified for a longer time are fetched in every
   * cycle because of an error in the SYNC_DELTA calculation of
   * {@link AdaptiveFetchSchedule}.
   * <br>
   * The next fetch time should always be in the future, never in the past.
   * </p>
   */
  @Test
  public void testAdaptiveFetchScheduleSyncDelta() {
    LOG.info("NUTCH-1564 test SYNC_DELTA calculation of AdaptiveFetchSchedule");
    Configuration conf = CrawlDBTestUtil.createConfiguration();
    conf.setLong("db.fetch.interval.default",               172800); // 2 days
    conf.setLong("db.fetch.schedule.adaptive.min_interval",  86400); // 1 day
    conf.setLong("db.fetch.schedule.adaptive.max_interval", 604800); // 7 days
    conf.setLong("db.fetch.interval.max",                   604800); // 7 days
    conf.set("db.fetch.schedule.class",
        "org.apache.nutch.crawl.AdaptiveFetchSchedule");
    ContinuousCrawlTestUtil crawlUtil = new CrawlTestFetchScheduleNotModifiedFetchTime(
        conf);
    crawlUtil.setInterval(FetchSchedule.SECONDS_PER_DAY/3);
    if (!crawlUtil.run(100)) {
      fail("failed: sync_delta calculation with AdaptiveFetchSchedule");
    }
  }

  private class CrawlTestFetchScheduleNotModifiedFetchTime extends
      CrawlTestFetchNotModified {

    // time of current fetch
    private long fetchTime;

    private long minInterval;
    private long maxInterval;

    CrawlTestFetchScheduleNotModifiedFetchTime(Configuration conf) {
      super(conf);
      minInterval = conf.getLong("db.fetch.schedule.adaptive.min_interval",
          86400); // 1 day
      maxInterval = conf.getLong("db.fetch.schedule.adaptive.max_interval",
          604800); // 7 days
      if (conf.getLong("db.fetch.interval.max", 604800) < maxInterval) {
        maxInterval = conf.getLong("db.fetch.interval.max", 604800);
      }
    }

    @Override
    protected CrawlDatum fetch(CrawlDatum datum, long currentTime) {
      // remember time of fetching
      fetchTime = currentTime;
      return super.fetch(datum, currentTime);
    }

    @Override
    protected boolean check(CrawlDatum result) {
      if (result.getStatus() == STATUS_DB_NOTMODIFIED) {
        // check only status notmodified here
        long secondsUntilNextFetch = (result.getFetchTime() - fetchTime) / 1000L;
        if (secondsUntilNextFetch < -1) {
          // next fetch time is in the past (more than one second)
          LOG.error("Next fetch time is in the past: " + result);
          return false;
        }
        if (secondsUntilNextFetch < 60) {
          // next fetch time is in less than one minute
          // (critical: Nutch can hardly be so fast)
          LOG.error("Less then one minute until next fetch: " + result);
       }
        // Next fetch time should be within min. and max. (tolerance: 60 sec.)
        if (secondsUntilNextFetch+60 < minInterval
            || secondsUntilNextFetch-60 > maxInterval) {
          LOG.error("Interval until next fetch time ("
              + TimingUtil.elapsedTime(fetchTime, result.getFetchTime())
              + ") is not within min. and max. interval: " + result);
          // TODO: is this a failure?
        }
      }
      return true;
    }

  }

  /**
   * Test whether signatures are reset for "content-less" states
   * (gone, redirect, etc.): otherwise, if this state is temporary
   * and the document appears again with the old content, it may
   * get marked as not_modified in CrawlDb just after the redirect
   * state. In this case we cannot expect content in segments.
   * Cf. NUTCH-1422: reset signature for redirects.
   */
  // TODO: can only test if solution is done in CrawlDbReducer
  @Test
  public void testSignatureReset() {
    LOG.info("NUTCH-1422 must reset signature for redirects and similar states");
    Configuration conf = CrawlDBTestUtil.createConfiguration();
    for (String sched : schedules) {
      LOG.info("Testing reset signature with " + sched);
      conf.set("db.fetch.schedule.class", "org.apache.nutch.crawl."+sched);
      ContinuousCrawlTestUtil crawlUtil = new CrawlTestSignatureReset(conf);
      if (!crawlUtil.run(20)) {
        fail("failed: signature not reset");
      }
    }
  }

  private class CrawlTestSignatureReset extends ContinuousCrawlTestUtil {

    byte[][] noContentStates = {
        { STATUS_FETCH_GONE,       STATUS_DB_GONE },
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
      if (counter%2 == 1) {
        fetchState = STATUS_FETCH_SUCCESS;
      } else {
        fetchState = noContentStates[(counter%6)/2][0];
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
