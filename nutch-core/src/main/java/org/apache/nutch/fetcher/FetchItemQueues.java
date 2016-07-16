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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class - a collection of queues that keeps track of the total
 * number of items, and provides items eligible for fetching from any queue.
 */
public class FetchItemQueues {

  private static final Logger LOG = LoggerFactory.getLogger(FetchItemQueues.class);
  
  public static final String DEFAULT_ID = "default";
  Map<String, FetchItemQueue> queues = new HashMap<String, FetchItemQueue>();
  AtomicInteger totalSize = new AtomicInteger(0);
  int maxThreads;
  long crawlDelay;
  long minCrawlDelay;
  long timelimit = -1;
  int maxExceptionsPerQueue = -1;
  Configuration conf;

  public static final String QUEUE_MODE_HOST = "byHost";
  public static final String QUEUE_MODE_DOMAIN = "byDomain";
  public static final String QUEUE_MODE_IP = "byIP";

  String queueMode;

  public FetchItemQueues(Configuration conf) {
    this.conf = conf;
    this.maxThreads = conf.getInt("fetcher.threads.per.queue", 1);
    queueMode = conf.get("fetcher.queue.mode", QUEUE_MODE_HOST);
    // check that the mode is known
    if (!queueMode.equals(QUEUE_MODE_IP)
        && !queueMode.equals(QUEUE_MODE_DOMAIN)
        && !queueMode.equals(QUEUE_MODE_HOST)) {
      LOG.error("Unknown partition mode : " + queueMode
          + " - forcing to byHost");
      queueMode = QUEUE_MODE_HOST;
    }
    LOG.info("Using queue mode : " + queueMode);

    this.crawlDelay = (long) (conf.getFloat("fetcher.server.delay", 1.0f) * 1000);
    this.minCrawlDelay = (long) (conf.getFloat("fetcher.server.min.delay",
        0.0f) * 1000);
    this.timelimit = conf.getLong("fetcher.timelimit", -1);
    this.maxExceptionsPerQueue = conf.getInt(
        "fetcher.max.exceptions.per.queue", -1);
  }

  public int getTotalSize() {
    return totalSize.get();
  }

  public int getQueueCount() {
    return queues.size();
  }

  public void addFetchItem(Text url, CrawlDatum datum) {
    FetchItem it = FetchItem.create(url, datum, queueMode);
    if (it != null)
      addFetchItem(it);
  }

  public synchronized void addFetchItem(FetchItem it) {
    FetchItemQueue fiq = getFetchItemQueue(it.queueID);
    fiq.addFetchItem(it);
    totalSize.incrementAndGet();
  }

  public void finishFetchItem(FetchItem it) {
    finishFetchItem(it, false);
  }

  public void finishFetchItem(FetchItem it, boolean asap) {
    FetchItemQueue fiq = queues.get(it.queueID);
    if (fiq == null) {
      LOG.warn("Attempting to finish item from unknown queue: " + it);
      return;
    }
    fiq.finishFetchItem(it, asap);
  }

  public synchronized FetchItemQueue getFetchItemQueue(String id) {
    FetchItemQueue fiq = queues.get(id);
    if (fiq == null) {
      // initialize queue
      fiq = new FetchItemQueue(conf, maxThreads, crawlDelay, minCrawlDelay);
      queues.put(id, fiq);
    }
    return fiq;
  }

  public synchronized FetchItem getFetchItem() {
    Iterator<Map.Entry<String, FetchItemQueue>> it = queues.entrySet()
        .iterator();
    while (it.hasNext()) {
      FetchItemQueue fiq = it.next().getValue();
      // reap empty queues
      if (fiq.getQueueSize() == 0 && fiq.getInProgressSize() == 0) {
        it.remove();
        continue;
      }
      FetchItem fit = fiq.getFetchItem();
      if (fit != null) {
        totalSize.decrementAndGet();
        return fit;
      }
    }
    return null;
  }

  // called only once the feeder has stopped
  public synchronized int checkTimelimit() {
    int count = 0;

    if (System.currentTimeMillis() >= timelimit && timelimit != -1) {
      // emptying the queues
      count = emptyQueues();

      // there might also be a case where totalsize !=0 but number of queues
      // == 0
      // in which case we simply force it to 0 to avoid blocking
      if (totalSize.get() != 0 && queues.size() == 0)
        totalSize.set(0);
    }
    return count;
  }

  // empties the queues (used by timebomb and throughput threshold)
  public synchronized int emptyQueues() {
    int count = 0;

    for (String id : queues.keySet()) {
      FetchItemQueue fiq = queues.get(id);
      if (fiq.getQueueSize() == 0)
        continue;
      LOG.info("* queue: " + id + " >> dropping! ");
      int deleted = fiq.emptyQueue();
      for (int i = 0; i < deleted; i++) {
        totalSize.decrementAndGet();
      }
      count += deleted;
    }

    return count;
  }

  /**
   * Increment the exception counter of a queue in case of an exception e.g.
   * timeout; when higher than a given threshold simply empty the queue.
   * 
   * @param queueid
   * @return number of purged items
   */
  public synchronized int checkExceptionThreshold(String queueid) {
    FetchItemQueue fiq = queues.get(queueid);
    if (fiq == null) {
      return 0;
    }
    if (fiq.getQueueSize() == 0) {
      return 0;
    }
    int excCount = fiq.incrementExceptionCounter();
    if (maxExceptionsPerQueue != -1 && excCount >= maxExceptionsPerQueue) {
      // too many exceptions for items in this queue - purge it
      int deleted = fiq.emptyQueue();
      LOG.info("* queue: " + queueid + " >> removed " + deleted
          + " URLs from queue because " + excCount + " exceptions occurred");
      for (int i = 0; i < deleted; i++) {
        totalSize.decrementAndGet();
      }
      return deleted;
    }
    return 0;
  }

  public synchronized void dump() {
    for (String id : queues.keySet()) {
      FetchItemQueue fiq = queues.get(id);
      if (fiq.getQueueSize() == 0)
        continue;
      LOG.info("* queue: " + id);
      fiq.dump();
    }
  }
}
