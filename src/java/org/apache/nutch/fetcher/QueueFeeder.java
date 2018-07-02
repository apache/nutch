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
package org.apache.nutch.fetcher;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.fetcher.Fetcher.FetcherRun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class feeds the queues with input items, and re-fills them as items
 * are consumed by FetcherThread-s.
 */
public class QueueFeeder extends Thread {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  
  private FetcherRun.Context context;
  private FetchItemQueues queues;
  private int size;
  private long timelimit = -1;

  public QueueFeeder(FetcherRun.Context context,
      FetchItemQueues queues, int size) {
    this.context = context;
    this.queues = queues;
    this.size = size;
    this.setDaemon(true);
    this.setName("QueueFeeder");
  }

  public void setTimeLimit(long tl) {
    timelimit = tl;
  }

  public void run() {
    boolean hasMore = true;
    int cnt = 0;
    int timelimitcount = 0;
    while (hasMore) {
      if (System.currentTimeMillis() >= timelimit && timelimit != -1) {
        // enough .. lets' simply
        // read all the entries from the input without processing them
        try {
          hasMore = context.nextKeyValue();
          timelimitcount++;
        } catch (IOException e) {
          LOG.error("QueueFeeder error reading input, record " + cnt, e);
          return;
        } catch (InterruptedException e) {
          LOG.info("QueueFeeder interrupted, exception:", e);
          return;
        }
        continue;
      }
      int feed = size - queues.getTotalSize();
      if (feed <= 0) {
        // queues are full - spin-wait until they have some free space
        try {
          Thread.sleep(1000);
        } catch (Exception e) {
        }
        ;
        continue;
      } else {
        LOG.debug("-feeding {} input urls ...", feed);
        while (feed > 0 && hasMore) {
          try {
            hasMore = context.nextKeyValue();
            if (hasMore) {
              /*
               * Need to copy key and value objects because MapReduce will reuse
               * the original objects while the objects are stored in the queue.
               */
              Text url = new Text((Text)context.getCurrentKey());
              CrawlDatum datum = new CrawlDatum();
              datum.set((CrawlDatum)context.getCurrentValue());
              queues.addFetchItem(url, datum);
              cnt++;
              feed--;
            }
          } catch (IOException e) {
            LOG.error("QueueFeeder error reading input, record " + cnt, e);
            return;
          } catch (InterruptedException e) {
            LOG.info("QueueFeeder interrupted, exception:", e);
          }
        }
      }
    }
    LOG.info("QueueFeeder finished: total {} records hit by time limit : {}",
        cnt, timelimitcount);
  }
}
