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
package org.apache.nutch.hostdb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.nutch.crawl.CrawlDatum;

/**
 * Simple custom crawl datum processor that counts the number of records that
 * are overdue for fetching, e.g. new unfetched URLs that haven't been fetched
 * within two days.
 */
public class FetchOverdueCrawlDatumProcessor implements CrawlDatumProcessor {

  protected final Configuration conf;
  protected long overDueTimeLimit = 60 * 60 * 24 * 2 * 1000l; // two days
  protected long overDueTime;
  protected long numOverDue = 0;

  public FetchOverdueCrawlDatumProcessor(Configuration conf) {
    this.conf = conf;

    overDueTimeLimit = conf.getLong("crawl.datum.processor.overdue.time.limit",
        60 * 60 * 24 * 2 * 1000l);
    overDueTime = System.currentTimeMillis() - overDueTimeLimit;
  }

  @Override
  public void count(CrawlDatum crawlDatum) {
    // Only compute overdue time for unfetched records
    if (crawlDatum.getStatus() == CrawlDatum.STATUS_DB_UNFETCHED) {
      // Does this fetch time exceed overDueTime?

      if (crawlDatum.getFetchTime() < overDueTime) {
        numOverDue++;
      }
    }
  }

  @Override
  public void finalize(HostDatum hostDatum) {
    hostDatum.getMetaData().put(new Text("num.overdue"),
        new LongWritable(numOverDue));
  }
}