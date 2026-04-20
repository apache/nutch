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
package org.apache.nutch.metrics;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.Counters;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test utility for latency-tracking tests. Reduces boilerplate when testing
 * Fetcher, ParseSegment, and Indexer reducers that merge TDigests and set
 * job-level percentile counters.
 *
 * <p>Use with the real Hadoop {@link Counters} from {@link org.apache.nutch.util.ReducerContextWrapper#getCounters()}
 * (no mocks).
 */
public final class LatencyTestUtil {

  private static final String DUMMY_GROUP = "test";
  private static final String DUMMY_PREFIX = "latency";

  private LatencyTestUtil() {}

  /**
   * Builds serialized TDigest bytes from the given samples. Uses a temporary
   * LatencyTracker with dummy group/prefix. Callers wrap the result as needed
   * (e.g. {@code new BytesWritable(bytes)} or {@code new NutchWritable(new BytesWritable(bytes))}).
   *
   * @param samples latency values in milliseconds to record
   * @return serialized digest as from {@link LatencyTracker#toBytes()}
   */
  public static byte[] createDigestBytes(long... samples) {
    LatencyTracker tracker = new LatencyTracker(DUMMY_GROUP, DUMMY_PREFIX);
    for (long sample : samples) {
      tracker.record(sample);
    }
    return tracker.toBytes();
  }

  /**
   * Builds one BytesWritable per array of samples (e.g. one per map task).
   * Useful for reducer tests that merge multiple digests.
   *
   * @param sampleArrays each array is recorded into one tracker and serialized to one BytesWritable
   * @return list of digest BytesWritable, in order
   */
  public static List<BytesWritable> createDigestBytesWritables(long[]... sampleArrays) {
    List<BytesWritable> list = new ArrayList<>(sampleArrays.length);
    for (long[] samples : sampleArrays) {
      list.add(new BytesWritable(createDigestBytes(samples)));
    }
    return list;
  }

  /**
   * Asserts that the job-level percentile counters (p50, p95, p99) for the
   * given group and prefix are in the range [minMs, maxMs]. Uses
   * {@link LatencyTracker#SUFFIX_P50_MS} etc.
   *
   * @param counters counters from {@link org.apache.nutch.util.ReducerContextWrapper#getCounters()}
   * @param group    counter group (e.g. {@link NutchMetrics#GROUP_FETCHER})
   * @param prefix   counter name prefix (e.g. {@link NutchMetrics#FETCHER_LATENCY})
   * @param minMs    inclusive lower bound for all percentiles (ms)
   * @param maxMs    inclusive upper bound for all percentiles (ms)
   */
  public static void assertPercentilesInRange(Counters counters, String group, String prefix,
      long minMs, long maxMs) {
    long p50 = counters.findCounter(group, prefix + LatencyTracker.SUFFIX_P50_MS).getValue();
    long p95 = counters.findCounter(group, prefix + LatencyTracker.SUFFIX_P95_MS).getValue();
    long p99 = counters.findCounter(group, prefix + LatencyTracker.SUFFIX_P99_MS).getValue();
    assertTrue(p50 >= minMs && p50 <= maxMs,
        "p50=" + p50 + " not in [" + minMs + "," + maxMs + "]");
    assertTrue(p95 >= minMs && p95 <= maxMs,
        "p95=" + p95 + " not in [" + minMs + "," + maxMs + "]");
    assertTrue(p99 >= minMs && p99 <= maxMs,
        "p99=" + p99 + " not in [" + minMs + "," + maxMs + "]");
  }

  /**
   * Asserts that the count and sum counters for the given group and prefix
   * match the expected values. Uses {@link LatencyTracker#SUFFIX_COUNT_TOTAL}
   * and {@link LatencyTracker#SUFFIX_SUM_MS}.
   *
   * @param counters       counters from {@link org.apache.nutch.util.ReducerContextWrapper#getCounters()}
   * @param group          counter group
   * @param prefix         counter name prefix
   * @param expectedCount  expected _count_total value
   * @param expectedSumMs  expected _sum_ms value
   */
  public static void assertCountAndSum(Counters counters, String group, String prefix,
      long expectedCount, long expectedSumMs) {
    assertEquals(expectedCount,
        counters.findCounter(group, prefix + LatencyTracker.SUFFIX_COUNT_TOTAL).getValue());
    assertEquals(expectedSumMs,
        counters.findCounter(group, prefix + LatencyTracker.SUFFIX_SUM_MS).getValue());
  }

  /**
   * Asserts that the percentile counters (p50, p95, p99) for the given group
   * and prefix are all zero. Useful for tests that emit with zero samples.
   *
   * @param counters counters from {@link org.apache.nutch.util.ReducerContextWrapper#getCounters()}
   * @param group    counter group
   * @param prefix   counter name prefix
   */
  public static void assertPercentilesZero(Counters counters, String group, String prefix) {
    assertEquals(0, counters.findCounter(group, prefix + LatencyTracker.SUFFIX_P50_MS).getValue());
    assertEquals(0, counters.findCounter(group, prefix + LatencyTracker.SUFFIX_P95_MS).getValue());
    assertEquals(0, counters.findCounter(group, prefix + LatencyTracker.SUFFIX_P99_MS).getValue());
  }

  /**
   * Asserts that count, sum, and all percentile counters are zero.
   *
   * @param counters counters from {@link org.apache.nutch.util.ReducerContextWrapper#getCounters()}
   * @param group    counter group
   * @param prefix   counter name prefix
   */
  public static void assertCountSumAndPercentilesZero(Counters counters, String group, String prefix) {
    assertCountAndSum(counters, group, prefix, 0, 0);
    assertPercentilesZero(counters, group, prefix);
  }
}
