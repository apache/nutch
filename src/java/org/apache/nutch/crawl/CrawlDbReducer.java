/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.Iterator;
import java.io.IOException;

import org.apache.nutch.io.*;
import org.apache.nutch.mapred.*;

/** Merge new page entries with existing entries. */
public class CrawlDbReducer implements Reducer {
  private int retryMax;

  public void configure(JobConf job) {
    retryMax = job.getInt("db.fetch.retry.max", 3);
  }

  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter)
    throws IOException {

    CrawlDatum highest = null;
    CrawlDatum old = null;
    byte[] signature = null;
    float scoreIncrement = 0.0f;

    while (values.hasNext()) {
      CrawlDatum datum = (CrawlDatum)values.next();

      if (highest == null || datum.getStatus() > highest.getStatus()) {
        highest = datum;                          // find highest status
      }

      switch (datum.getStatus()) {                // find old entry, if any
      case CrawlDatum.STATUS_DB_UNFETCHED:
      case CrawlDatum.STATUS_DB_FETCHED:
        old = datum;
        break;
      case CrawlDatum.STATUS_LINKED:
        scoreIncrement += datum.getScore();
        break;
      case CrawlDatum.STATUS_SIGNATURE:
        signature = datum.getSignature();
      }
    }

    CrawlDatum result = null;

    switch (highest.getStatus()) {                // determine new status

    case CrawlDatum.STATUS_DB_UNFETCHED:          // no new entry
    case CrawlDatum.STATUS_DB_FETCHED:
    case CrawlDatum.STATUS_DB_GONE:
      result = old;                               // use old
      break;

    case CrawlDatum.STATUS_LINKED:                // highest was link
      if (old != null) {                          // if old exists
        result = old;                             // use it
      } else {
        result = highest;                         // use new entry
        result.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
        result.setScore(1.0f);                    // initial score is 1.0f
      }
      result.setSignature(null);                  // reset the signature
      break;
      
    case CrawlDatum.STATUS_FETCH_SUCCESS:         // succesful fetch
      result = highest;                           // use new entry
      if (highest.getSignature() == null) highest.setSignature(signature);
      result.setStatus(CrawlDatum.STATUS_DB_FETCHED);
      result.setNextFetchTime();
      break;

    case CrawlDatum.STATUS_FETCH_RETRY:           // temporary failure
      result = highest;                           // use new entry
      if (old != null)
        result.setSignature(old.getSignature());  // use old signature
      if (highest.getRetriesSinceFetch() < retryMax) {
        result.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
      } else {
        result.setStatus(CrawlDatum.STATUS_DB_GONE);
      }
      break;

    case CrawlDatum.STATUS_FETCH_GONE:            // permanent failure
      result = highest;                           // use new entry
      if (old != null)
        result.setSignature(old.getSignature());  // use old signature
      result.setStatus(CrawlDatum.STATUS_DB_GONE);
      break;

    default:
      throw new RuntimeException("Unknown status: "+highest.getStatus());
    }
    
    if (result != null) {
      result.setScore(result.getScore() + scoreIncrement);
      output.collect(key, result);
    }
  }

}
