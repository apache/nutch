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

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * A collection of queues that keeps track of the total number of items, and
 * provides items eligible for fetching from any queue.
 */
public class FetchItemQueues {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static final String DEFAULT_ID = "default";
  Map<String, FetchItemQueue> queues = new ConcurrentHashMap<>();
  private Set<String> queuesMaxExceptions = new HashSet<>();
  Iterator<Map.Entry<String, FetchItemQueue>> lastIterator = null;
  AtomicInteger totalSize = new AtomicInteger(0);
  Cache<Text, Optional<String>> redirectDedupCache = null;

  int maxThreads;
  long crawlDelay;
  long minCrawlDelay;
  long timelimit = -1;
  int maxExceptionsPerQueue = -1;
  long exceptionsPerQueueDelay = -1;
  boolean feederAlive = true;
  Configuration conf;

  public static final String QUEUE_MODE_HOST = "byHost";
  public static final String QUEUE_MODE_DOMAIN = "byDomain";
  public static final String QUEUE_MODE_IP = "byIP";

  String queueMode;

  enum QueuingStatus {
    SUCCESSFULLY_QUEUED,
    ERROR_CREATE_FETCH_ITEM,
    ABOVE_EXCEPTION_THRESHOLD,
    HIT_BY_TIMELIMIT;
  }

  public FetchItemQueues(Configuration conf) {
    this.conf = conf;
    this.maxThreads = conf.getInt("fetcher.threads.per.queue", 1);
    queueMode = conf.get("fetcher.queue.mode", QUEUE_MODE_HOST);
    queueMode = checkQueueMode(queueMode);
    LOG.info("Using queue mode : " + queueMode);

    this.crawlDelay = (long) (conf.getFloat("fetcher.server.delay", 1.0f) * 1000);
    this.minCrawlDelay = (long) (conf.getFloat("fetcher.server.min.delay",
        0.0f) * 1000);
    this.timelimit = conf.getLong("fetcher.timelimit", -1);
    this.maxExceptionsPerQueue = conf.getInt(
        "fetcher.max.exceptions.per.queue", -1);
    this.exceptionsPerQueueDelay = (long) (conf
        .getFloat("fetcher.exceptions.per.queue.delay", .0f) * 1000);

    int dedupRedirMaxTime = conf.getInt("fetcher.redirect.dedupcache.seconds",
        -1);
    int dedupRedirMaxSize = conf.getInt("fetcher.redirect.dedupcache.size",
        1000);
    if (dedupRedirMaxTime > 0 && dedupRedirMaxSize > 0) {
      redirectDedupCache = CacheBuilder.newBuilder()
          .maximumSize(dedupRedirMaxSize)
          .expireAfterWrite(dedupRedirMaxTime, TimeUnit.SECONDS).build();
    }
  }

  /**
   * Check whether queue mode is valid, fall-back to default mode if not.
   * 
   * @param queueMode
   *          queue mode to check
   * @return valid queue mode or default
   */
  protected static String checkQueueMode(String queueMode) {
    // check that the mode is known
    if (!queueMode.equals(QUEUE_MODE_IP)
        && !queueMode.equals(QUEUE_MODE_DOMAIN)
        && !queueMode.equals(QUEUE_MODE_HOST)) {
      LOG.error("Unknown partition mode : {} - forcing to byHost", queueMode);
      queueMode = QUEUE_MODE_HOST;
    }
    return queueMode;
  }

  public int getTotalSize() {
    return totalSize.get();
  }

  public int getQueueCount() {
    return queues.size();
  }

  public int getQueueCountMaxExceptions() {
    return queuesMaxExceptions.size();
  }

  public QueuingStatus addFetchItem(Text url, CrawlDatum datum) {
    FetchItem it = FetchItem.create(url, datum, queueMode);
    if (it != null) {
      return addFetchItem(it);
    }
    return QueuingStatus.ERROR_CREATE_FETCH_ITEM;
  }

  public synchronized QueuingStatus addFetchItem(FetchItem it) {
    if (maxExceptionsPerQueue != -1
        && queuesMaxExceptions.contains(it.queueID)) {
      return QueuingStatus.ABOVE_EXCEPTION_THRESHOLD;
    }
    FetchItemQueue fiq = getFetchItemQueue(it.queueID);
    fiq.addFetchItem(it);
    totalSize.incrementAndGet();
    return QueuingStatus.SUCCESSFULLY_QUEUED;
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

    Iterator<Map.Entry<String, FetchItemQueue>> it = lastIterator;
    if (it == null || !it.hasNext()) {
      it = queues.entrySet().iterator();
    }

    while (it.hasNext()) {
      FetchItemQueue fiq = it.next().getValue();

      // reap empty queues which do not hold state required to ensure politeness
      if (fiq.getQueueSize() == 0 && fiq.getInProgressSize() == 0) {
        if (!feederAlive) {
          // no more fetch items added
          it.remove();
        } else if ((maxExceptionsPerQueue > -1 || exceptionsPerQueueDelay > 0)
            && fiq.exceptionCounter.get() > 0) {
          // keep queue because the exceptions counter is bound to it
          // and is required to skip or delay items on this queue
        } else if (fiq.nextFetchTime.get() > System.currentTimeMillis()) {
          // keep queue to have it blocked in case new fetch items of this queue
          // are added by the QueueFeeder
        } else {
          // empty queue without state
          it.remove();
        }
        continue;
      }

      FetchItem fit = fiq.getFetchItem();
      if (fit != null) {
        totalSize.decrementAndGet();
        lastIterator = it;
        return fit;
      }
    }

    lastIterator = null;
    return null;
  }

  /**
   * @return true if the fetcher timelimit is defined and has been exceeded
   *         ({@code fetcher.timelimit.mins} minutes after fetching started)
   */
  public boolean timelimitExceeded() {
    return timelimit != -1 && System.currentTimeMillis() >= timelimit;
  }

  // called only once the feeder has stopped
  public synchronized int checkTimelimit() {
    int count = 0;

    if (timelimitExceeded()) {
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
      LOG.info("* queue: {} >> dropping!", id);
      int deleted = fiq.emptyQueue();
      totalSize.addAndGet(-deleted);
      count += deleted;
    }

    return count;
  }

  /**
   * Increment the exception counter of a queue in case of an exception e.g.
   * timeout; when higher than a given threshold simply empty the queue.
   * 
   * The next fetch is delayed if specified by the param {@code delay} or
   * configured by the property {@code fetcher.exceptions.per.queue.delay}.
   * 
   * @param queueid
   *          a queue identifier to locate and check
   * @param maxExceptions
   *          custom-defined number of max. exceptions - if negative the value
   *          of the property {@code fetcher.max.exceptions.per.queue} is used.
   * @param delay
   *          a custom-defined time span in milliseconds to delay the next fetch
   *          in addition to the delay defined for the given queue. If a
   *          negative value is passed the delay is chosen by
   *          {@code fetcher.exceptions.per.queue.delay}
   * 
   * @return number of purged items
   */
  public synchronized int checkExceptionThreshold(String queueid,
      int maxExceptions, long delay) {
    FetchItemQueue fiq = queues.get(queueid);
    if (fiq == null) {
      return 0;
    }
    int excCount = fiq.incrementExceptionCounter();
    if (delay > 0) {
      fiq.nextFetchTime.getAndAdd(delay);
      LOG.info("* queue: {} >> delayed next fetch by {} ms", queueid, delay);
    } else if (exceptionsPerQueueDelay > 0) {
      /*
       * Delay the next fetch by a time span growing exponentially with the
       * number of observed exceptions. This dynamic delay is added to the
       * constant delay. In order to avoid overflows, the exponential backoff is
       * capped at 2**31
       */
      long exceptionDelay = exceptionsPerQueueDelay;
      if (excCount > 1) {
        // double the initial delay with every observed exception
        exceptionDelay *= 2L << Math.min((excCount - 2), 31);
      }
      fiq.nextFetchTime.getAndAdd(exceptionDelay);
      LOG.info(
          "* queue: {} >> delayed next fetch by {} ms after {} exceptions in queue",
          queueid, exceptionDelay, excCount);
    }
    if (fiq.getQueueSize() == 0) {
      return 0;
    }
    if (maxExceptions!= -1 && excCount >= maxExceptions) {
      // too many exceptions for items in this queue - purge it
      int deleted = fiq.emptyQueue();
      LOG.info(
          "* queue: {} >> removed {} URLs from queue because {} exceptions occurred",
          queueid, deleted, excCount);
      totalSize.getAndAdd(-deleted);
      // keep queue IDs to ensure that these queues aren't created and filled
      // again, see addFetchItem(FetchItem)
      queuesMaxExceptions.add(queueid);
      return deleted;
    }
    return 0;
  }

  /**
   * Increment the exception counter of a queue in case of an exception e.g.
   * timeout; when higher than a given threshold simply empty the queue.
   * 
   * @see #checkExceptionThreshold(String, int, long)
   * 
   * @param queueid
   *          queue identifier to locate and check
   * @return number of purged items
   */
  public int checkExceptionThreshold(String queueid) {
    return checkExceptionThreshold(queueid, this.maxExceptionsPerQueue, -1);
  }

  /**
   * @param redirUrl
   *          redirect target
   * @return true if redirects are deduplicated and redirUrl has been queued
   *         recently
   */
  public boolean redirectIsQueuedRecently(Text redirUrl) {
    if (redirectDedupCache != null) {
      if (redirectDedupCache.getIfPresent(redirUrl) != null) {
        return true;
      }
      redirectDedupCache.put(redirUrl, Optional.absent());
    }
    return false;
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
