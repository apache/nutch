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
import java.util.Iterator;
import java.io.IOException;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;

/** Merge new page entries with existing entries. */
public class CrawlDbReducer implements Reducer {
  public static final Log LOG = LogFactory.getLog(CrawlDbReducer.class);
  
  private int retryMax;
  private CrawlDatum result = new CrawlDatum();
  private ArrayList<CrawlDatum> linked = new ArrayList<CrawlDatum>();
  private ScoringFilters scfilters = null;
  private boolean additionsAllowed;
  private int maxInterval;
  private FetchSchedule schedule;

  public void configure(JobConf job) {
    retryMax = job.getInt("db.fetch.retry.max", 3);
    scfilters = new ScoringFilters(job);
    additionsAllowed = job.getBoolean(CrawlDb.CRAWLDB_ADDITIONS_ALLOWED, true);
    int oldMaxInterval = job.getInt("db.max.fetch.interval", 0);
    maxInterval = job.getInt("db.fetch.interval.max", 0 );
    if (oldMaxInterval > 0 && maxInterval == 0) maxInterval = oldMaxInterval * FetchSchedule.SECONDS_PER_DAY;
    schedule = FetchScheduleFactory.getFetchSchedule(job);
  }

  public void close() {}

  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter)
    throws IOException {

    CrawlDatum fetch = null;
    CrawlDatum old = null;
    byte[] signature = null;
    linked.clear();

    while (values.hasNext()) {
      CrawlDatum datum = (CrawlDatum)values.next();
      if (CrawlDatum.hasDbStatus(datum)) {
        if (old == null) {
          old = datum;
        } else {
          // always take the latest version
          if (old.getFetchTime() < datum.getFetchTime()) old = datum;
        }
        continue;
      }

      if (CrawlDatum.hasFetchStatus(datum)) {
        if (fetch == null) {
          fetch = datum;
        } else {
          // always take the latest version
          if (fetch.getFetchTime() < datum.getFetchTime()) fetch = datum;
        }
        continue;
      }

      switch (datum.getStatus()) {                // collect other info
      case CrawlDatum.STATUS_LINKED:
        linked.add(datum);
        break;
      case CrawlDatum.STATUS_SIGNATURE:
        signature = datum.getSignature();
        break;
      default:
        LOG.warn("Unknown status, key: " + key + ", datum: " + datum);
      }
    }

    // if it doesn't already exist, skip it
    if (old == null && !additionsAllowed) return;
    
    // if there is no fetched datum, perhaps there is a link
    if (fetch == null && linked.size() > 0) {
      fetch = linked.get(0);
    }
    
    // still no new data - record only unchanged old data, if exists, and return
    if (fetch == null) {
      if (old != null) // at this point at least "old" should be present
        output.collect(key, old);
      else
        LOG.warn("Missing fetch and old value, signature=" + signature);
      return;
    }
    
    if (signature == null) signature = fetch.getSignature();
    long prevModifiedTime = old != null ? old.getModifiedTime() : 0L;
    long prevFetchTime = old != null ? old.getFetchTime() : 0L;

    // initialize with the latest version, be it fetch or link
    result.set(fetch);
    if (old != null) {
      // copy metadata from old, if exists
      if (old.getMetaData().size() > 0) {
        result.getMetaData().putAll(old.getMetaData());
        // overlay with new, if any
        if (fetch.getMetaData().size() > 0)
          result.getMetaData().putAll(fetch.getMetaData());
      }
      // set the most recent valid value of modifiedTime
      if (old.getModifiedTime() > 0 && fetch.getModifiedTime() == 0) {
        result.setModifiedTime(old.getModifiedTime());
      }
    }
    
    switch (fetch.getStatus()) {                // determine new status

    case CrawlDatum.STATUS_LINKED:                // it was link
      if (old != null) {                          // if old exists
        result.set(old);                          // use it
      } else {
        result = schedule.initializeSchedule((Text)key, result);
        result.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
        try {
          scfilters.initialScore((Text)key, result);
        } catch (ScoringFilterException e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Cannot filter init score for url " + key +
                     ", using default: " + e.getMessage());
          }
          result.setScore(0.0f);
        }
      }
      break;
      
    case CrawlDatum.STATUS_FETCH_SUCCESS:         // succesful fetch
    case CrawlDatum.STATUS_FETCH_REDIR_TEMP:      // successful fetch, redirected
    case CrawlDatum.STATUS_FETCH_REDIR_PERM:
    case CrawlDatum.STATUS_FETCH_NOTMODIFIED:     // successful fetch, notmodified
      // determine the modification status
      int modified = FetchSchedule.STATUS_UNKNOWN;
      if (fetch.getStatus() == CrawlDatum.STATUS_FETCH_NOTMODIFIED) {
        modified = FetchSchedule.STATUS_NOTMODIFIED;
      } else {
        if (old != null && old.getSignature() != null && signature != null) {
          if (SignatureComparator._compare(old.getSignature(), signature) != 0) {
            modified = FetchSchedule.STATUS_MODIFIED;
          } else {
            modified = FetchSchedule.STATUS_NOTMODIFIED;
          }
        }
      }
      // set the schedule
      result = schedule.setFetchSchedule((Text)key, result, prevFetchTime,
          prevModifiedTime, fetch.getFetchTime(), fetch.getModifiedTime(), modified);
      // set the result status and signature
      if (modified == FetchSchedule.STATUS_NOTMODIFIED) {
        result.setStatus(CrawlDatum.STATUS_DB_NOTMODIFIED);
        if (old != null) result.setSignature(old.getSignature());
      } else {
        switch (fetch.getStatus()) {
        case CrawlDatum.STATUS_FETCH_SUCCESS:
          result.setStatus(CrawlDatum.STATUS_DB_FETCHED);
          break;
        case CrawlDatum.STATUS_FETCH_REDIR_PERM:
          result.setStatus(CrawlDatum.STATUS_DB_REDIR_PERM);
          break;
        case CrawlDatum.STATUS_FETCH_REDIR_TEMP:
          result.setStatus(CrawlDatum.STATUS_DB_REDIR_TEMP);
          break;
        default:
          LOG.warn("Unexpected status: " + fetch.getStatus() + " resetting to old status.");
          if (old != null) result.setStatus(old.getStatus());
          else result.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
        }
        result.setSignature(signature);
      }
      // if fetchInterval is larger than the system-wide maximum, trigger
      // an unconditional recrawl. This prevents the page to be stuck at
      // NOTMODIFIED state, when the old fetched copy was already removed with
      // old segments.
      if (maxInterval < result.getFetchInterval())
        result = schedule.forceRefetch((Text)key, result, false);
      break;
    case CrawlDatum.STATUS_SIGNATURE:
      if (LOG.isWarnEnabled()) {
        LOG.warn("Lone CrawlDatum.STATUS_SIGNATURE: " + key);
      }   
      return;
    case CrawlDatum.STATUS_FETCH_RETRY:           // temporary failure
      if (old != null) {
        result.setSignature(old.getSignature());  // use old signature
      }
      result = schedule.setPageRetrySchedule((Text)key, result, prevFetchTime,
          prevModifiedTime, fetch.getFetchTime());
      if (result.getRetriesSinceFetch() < retryMax) {
        result.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
      } else {
        result.setStatus(CrawlDatum.STATUS_DB_GONE);
      }
      break;

    case CrawlDatum.STATUS_FETCH_GONE:            // permanent failure
      if (old != null)
        result.setSignature(old.getSignature());  // use old signature
      result.setStatus(CrawlDatum.STATUS_DB_GONE);
      result = schedule.setPageGoneSchedule((Text)key, result, prevFetchTime,
          prevModifiedTime, fetch.getFetchTime());
      break;

    default:
      throw new RuntimeException("Unknown status: " + fetch.getStatus() + " " + key);
    }

    try {
      scfilters.updateDbScore((Text)key, old, result, linked);
    } catch (Exception e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Couldn't update score, key=" + key + ": " + e);
      }
    }
    // remove generation time, if any
    result.getMetaData().remove(Nutch.WRITABLE_GENERATE_TIME_KEY);
    output.collect(key, result);
  }

}
