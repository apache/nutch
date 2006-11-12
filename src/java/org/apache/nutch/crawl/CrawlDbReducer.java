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
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;

/** Merge new page entries with existing entries. */
public class CrawlDbReducer implements Reducer {
  public static final Log LOG = LogFactory.getLog(CrawlDbReducer.class);
  private int retryMax;
  private CrawlDatum result = new CrawlDatum();
  private ArrayList linked = new ArrayList();
  private ScoringFilters scfilters = null;
  private boolean additionsAllowed;

  public void configure(JobConf job) {
    retryMax = job.getInt("db.fetch.retry.max", 3);
    scfilters = new ScoringFilters(job);
    additionsAllowed = job.getBoolean(CrawlDb.CRAWLDB_ADDITIONS_ALLOWED, true);
  }

  public void close() {}

  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter)
    throws IOException {

    CrawlDatum highest = null;
    CrawlDatum old = null;
    byte[] signature = null;
    linked.clear();

    while (values.hasNext()) {
      CrawlDatum datum = (CrawlDatum)values.next();

      if (highest == null || datum.getStatus() > highest.getStatus()) {
        highest = datum;                          // find highest status
      }

      switch (datum.getStatus()) {                // find old entry, if any
      case CrawlDatum.STATUS_DB_UNFETCHED:
      case CrawlDatum.STATUS_DB_FETCHED:
      case CrawlDatum.STATUS_DB_GONE:
        old = datum;
        break;
      case CrawlDatum.STATUS_LINKED:
        linked.add(datum);
        break;
      case CrawlDatum.STATUS_SIGNATURE:
        signature = datum.getSignature();
      }
    }

    // if it doesn't already exist, skip it
    if (old == null && !additionsAllowed) return;
    
    // initialize with the latest version
    result.set(highest);
    if (old != null) {
      // copy metadata from old, if exists
      if (old.getMetaData().size() > 0) {
        result.getMetaData().putAll(old.getMetaData());
        // overlay with new, if any
        if (highest.getMetaData().size() > 0)
          result.getMetaData().putAll(highest.getMetaData());
      }
      // set the most recent valid value of modifiedTime
      if (old.getModifiedTime() > 0 && highest.getModifiedTime() == 0) {
        result.setModifiedTime(old.getModifiedTime());
      }
    }

    switch (highest.getStatus()) {                // determine new status

    case CrawlDatum.STATUS_DB_UNFETCHED:          // no new entry
    case CrawlDatum.STATUS_DB_FETCHED:
    case CrawlDatum.STATUS_DB_GONE:
      result.set(old);                            // use old
      break;

    case CrawlDatum.STATUS_LINKED:                // highest was link
      if (old != null) {                          // if old exists
        result.set(old);                          // use it
      } else {
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
      if (highest.getSignature() == null) result.setSignature(signature);
      result.setStatus(CrawlDatum.STATUS_DB_FETCHED);
      result.setNextFetchTime();
      break;

    case CrawlDatum.STATUS_SIGNATURE:
      if (LOG.isWarnEnabled()) {
        LOG.warn("Lone CrawlDatum.STATUS_SIGNATURE: " + key);
      }   
      return;
    case CrawlDatum.STATUS_FETCH_RETRY:           // temporary failure
      if (old != null)
        result.setSignature(old.getSignature());  // use old signature
      if (highest.getRetriesSinceFetch() < retryMax) {
        result.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
      } else {
        result.setStatus(CrawlDatum.STATUS_DB_GONE);
      }
      break;

    case CrawlDatum.STATUS_FETCH_GONE:            // permanent failure
      if (old != null)
        result.setSignature(old.getSignature());  // use old signature
      result.setStatus(CrawlDatum.STATUS_DB_GONE);
      break;

    default:
      throw new RuntimeException("Unknown status: " + highest.getStatus() + " " + key);
    }

    try {
      scfilters.updateDbScore((Text)key, old, result, linked);
    } catch (Exception e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Couldn't update score, key=" + key + ": " + e);
      }
    }
    output.collect(key, result);
  }

}
