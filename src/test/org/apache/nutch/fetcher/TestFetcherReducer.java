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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.metrics.LatencyTracker;
import org.apache.nutch.metrics.NutchMetrics;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.ReducerContextWrapper;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link Fetcher.FetcherReducer}: latency key branch (merge TDigests,
 * set job-level counters) and pass-through branch (write url, datum).
 */
class TestFetcherReducer {

  @Test
  void testReduceLatencyKeyMergesDigestsAndSetsCounters() throws IOException, InterruptedException {
    Configuration conf = NutchConfiguration.create();
    Map<Text, NutchWritable> out = new HashMap<>();
    Fetcher.FetcherReducer reducer = new Fetcher.FetcherReducer();
    ReducerContextWrapper<Text, NutchWritable, Text, NutchWritable> wrapper =
        new ReducerContextWrapper<>(reducer, conf, out);

    LatencyTracker tracker = new LatencyTracker(NutchMetrics.GROUP_FETCHER, NutchMetrics.FETCHER_LATENCY);
    tracker.record(100);
    tracker.record(200);
    tracker.record(300);
    byte[] digestBytes = tracker.toBytes();
    List<NutchWritable> values = new ArrayList<>();
    values.add(new NutchWritable(new BytesWritable(digestBytes)));

    reducer.reduce(new Text(NutchMetrics.LATENCY_KEY), values, wrapper.getContext());

    long p50 = wrapper.getCounters().findCounter(NutchMetrics.GROUP_FETCHER,
        NutchMetrics.FETCHER_LATENCY + LatencyTracker.SUFFIX_P50_MS).getValue();
    long p95 = wrapper.getCounters().findCounter(NutchMetrics.GROUP_FETCHER,
        NutchMetrics.FETCHER_LATENCY + LatencyTracker.SUFFIX_P95_MS).getValue();
    long p99 = wrapper.getCounters().findCounter(NutchMetrics.GROUP_FETCHER,
        NutchMetrics.FETCHER_LATENCY + LatencyTracker.SUFFIX_P99_MS).getValue();
    assertTrue(p50 >= 100 && p50 <= 300);
    assertTrue(p95 >= 100 && p95 <= 300);
    assertTrue(p99 >= 100 && p99 <= 300);
    assertEquals(0, out.size());
  }

  @Test
  void testReduceLatencyKeyWithMultipleDigestsMergesAndSetsCounters() throws IOException, InterruptedException {
    Configuration conf = NutchConfiguration.create();
    Map<Text, NutchWritable> out = new HashMap<>();
    Fetcher.FetcherReducer reducer = new Fetcher.FetcherReducer();
    ReducerContextWrapper<Text, NutchWritable, Text, NutchWritable> wrapper =
        new ReducerContextWrapper<>(reducer, conf, out);

    LatencyTracker t1 = new LatencyTracker(NutchMetrics.GROUP_FETCHER, NutchMetrics.FETCHER_LATENCY);
    t1.record(10);
    LatencyTracker t2 = new LatencyTracker(NutchMetrics.GROUP_FETCHER, NutchMetrics.FETCHER_LATENCY);
    t2.record(90);
    List<NutchWritable> values = new ArrayList<>();
    values.add(new NutchWritable(new BytesWritable(t1.toBytes())));
    values.add(new NutchWritable(new BytesWritable(t2.toBytes())));

    reducer.reduce(new Text(NutchMetrics.LATENCY_KEY), values, wrapper.getContext());

    long p50 = wrapper.getCounters().findCounter(NutchMetrics.GROUP_FETCHER,
        NutchMetrics.FETCHER_LATENCY + LatencyTracker.SUFFIX_P50_MS).getValue();
    assertTrue(p50 >= 10 && p50 <= 90);
    assertEquals(0, out.size());
  }

  @Test
  void testReducePassThroughWritesKeyValue() throws IOException, InterruptedException {
    Configuration conf = NutchConfiguration.create();
    Map<Text, NutchWritable> out = new HashMap<>();
    Fetcher.FetcherReducer reducer = new Fetcher.FetcherReducer();
    ReducerContextWrapper<Text, NutchWritable, Text, NutchWritable> wrapper =
        new ReducerContextWrapper<>(reducer, conf, out);

    Text url = new Text("http://example.com/");
    CrawlDatum datum = new CrawlDatum(CrawlDatum.STATUS_FETCH_SUCCESS, 0, 0.0f);
    List<NutchWritable> values = Collections.singletonList(new NutchWritable(datum));

    reducer.reduce(url, values, wrapper.getContext());

    assertEquals(1, out.size());
    assertTrue(out.containsKey(url));
    assertEquals(datum, out.get(url).get());
  }
}
