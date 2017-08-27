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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles FetchItems which come from the same host ID (be it a
 * proto/hostname or proto/IP pair). It also keeps track of requests in
 * progress and elapsed time between requests.
 */
public class FetchItemQueue {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  List<FetchItem> queue = Collections
      .synchronizedList(new LinkedList<FetchItem>());
  AtomicInteger inProgress = new AtomicInteger();
  AtomicLong nextFetchTime = new AtomicLong();
  AtomicInteger exceptionCounter = new AtomicInteger();
  long crawlDelay;
  long minCrawlDelay;
  int maxThreads;
  Configuration conf;
  Text cookie;
  Text variableFetchDelayKey = new Text("_variableFetchDelay_");
  boolean variableFetchDelaySet = false;
  
  public FetchItemQueue(Configuration conf, int maxThreads, long crawlDelay,
      long minCrawlDelay) {
    this.conf = conf;
    this.maxThreads = maxThreads;
    this.crawlDelay = crawlDelay;
    this.minCrawlDelay = minCrawlDelay;
    // ready to start
    setEndTime(System.currentTimeMillis() - crawlDelay);
  }

  public synchronized int emptyQueue() {
    int presize = queue.size();
    queue.clear();
    return presize;
  }

  public int getQueueSize() {
    return queue.size();
  }

  public int getInProgressSize() {
    return inProgress.get();
  }

  public int incrementExceptionCounter() {
    return exceptionCounter.incrementAndGet();
  }

  public void finishFetchItem(FetchItem it, boolean asap) {
    if (it != null) {
      inProgress.decrementAndGet();
      setEndTime(System.currentTimeMillis(), asap);
    }
  }

  public void addFetchItem(FetchItem it) {
    if (it == null)
      return;

    // Check for variable crawl delay
    if (it.datum.getMetaData().containsKey(variableFetchDelayKey)) {
      if (!variableFetchDelaySet) {
        variableFetchDelaySet = true;
        crawlDelay = ((LongWritable)(it.datum.getMetaData().get(variableFetchDelayKey))).get();
        minCrawlDelay = ((LongWritable)(it.datum.getMetaData().get(variableFetchDelayKey))).get();
        setEndTime(System.currentTimeMillis() - crawlDelay);
      }
      
      // Remove it!
      it.datum.getMetaData().remove(variableFetchDelayKey);
    }
    queue.add(it);
  }

  public void addInProgressFetchItem(FetchItem it) {
    if (it == null)
      return;
    inProgress.incrementAndGet();
  }

  public FetchItem getFetchItem() {
    if (inProgress.get() >= maxThreads)
      return null;
    long now = System.currentTimeMillis();
    if (nextFetchTime.get() > now)
      return null;
    FetchItem it = null;
    if (queue.size() == 0)
      return null;
    try {
      it = queue.remove(0);
      inProgress.incrementAndGet();
    } catch (Exception e) {
      LOG.error(
          "Cannot remove FetchItem from queue or cannot add it to inProgress queue",
          e);
    }
    return it;
  }
  
  public void setCookie(Text cookie) {
    this.cookie = cookie;
  }
  
  public Text getCookie() {
    return cookie;
  }

  public synchronized void dump() {
    LOG.info("  maxThreads    = " + maxThreads);
    LOG.info("  inProgress    = " + inProgress.get());
    LOG.info("  crawlDelay    = " + crawlDelay);
    LOG.info("  minCrawlDelay = " + minCrawlDelay);
    LOG.info("  nextFetchTime = " + nextFetchTime.get());
    LOG.info("  now           = " + System.currentTimeMillis());
    for (int i = 0; i < queue.size(); i++) {
      FetchItem it = queue.get(i);
      LOG.info("  " + i + ". " + it.url);
    }
  }

  private void setEndTime(long endTime) {
    setEndTime(endTime, false);
  }

  private void setEndTime(long endTime, boolean asap) {
    if (!asap)
      nextFetchTime.set(endTime
          + (maxThreads > 1 ? minCrawlDelay : crawlDelay));
    else
      nextFetchTime.set(endTime);
  }
}
