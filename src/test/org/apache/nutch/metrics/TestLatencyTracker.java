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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.ReducerContextWrapper;
import org.junit.jupiter.api.Test;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

/**
 * Unit tests for {@link LatencyTracker} merge, serialization (toBytes/fromBytes),
 * percentile behavior, and counter emission. Counter-emitting tests use the real
 * Hadoop Context and Counters via {@link ReducerContextWrapper} (no mocks of Hadoop).
 */
class TestLatencyTracker {

  private static final String GROUP = "test";
  private static final String PREFIX = "test_latency";

  @Test
  void testMergeAggregatesCountAndSum() {
    LatencyTracker a = new LatencyTracker(GROUP, PREFIX);
    a.record(10);
    a.record(20);
    LatencyTracker b = new LatencyTracker(GROUP, PREFIX);
    b.record(30);
    a.merge(b);
    assertEquals(3, a.getCount());
    assertEquals(60, a.getSum());
  }

  @Test
  void testMergeNullOrEmptyIsNoOp() {
    LatencyTracker a = new LatencyTracker(GROUP, PREFIX);
    a.record(5);
    long countBefore = a.getCount();
    a.merge(null);
    assertEquals(countBefore, a.getCount());
    LatencyTracker empty = new LatencyTracker(GROUP, PREFIX);
    a.merge(empty);
    assertEquals(countBefore, a.getCount());
  }

  @Test
  void testToBytesFromBytesRoundTrip() {
    LatencyTracker tracker = new LatencyTracker(GROUP, PREFIX);
    tracker.record(100);
    tracker.record(200);
    tracker.record(300);
    byte[] bytes = tracker.toBytes();
    assertNotNull(bytes);
    assertEquals(3, tracker.getCount());
    com.tdunning.math.stats.MergingDigest restored = LatencyTracker.fromBytes(bytes);
    assertNotNull(restored);
    double q50 = restored.quantile(0.50);
    assertTrue(q50 >= 100 && q50 <= 300);
  }

  @Test
  void testToBytesEmptyReturnsEmptyArray() {
    LatencyTracker tracker = new LatencyTracker(GROUP, PREFIX);
    byte[] bytes = tracker.toBytes();
    assertNotNull(bytes);
    assertEquals(0, bytes.length);
  }

  @Test
  void testFromBytesNullOrEmptyReturnsNull() {
    assertNull(LatencyTracker.fromBytes(null));
    assertNull(LatencyTracker.fromBytes(new byte[0]));
  }

  @Test
  void testGetPercentileReturnsValueInRange() {
    LatencyTracker tracker = new LatencyTracker(GROUP, PREFIX);
    tracker.record(100);
    tracker.record(200);
    tracker.record(300);
    long p50 = tracker.getPercentile(0.50);
    long p95 = tracker.getPercentile(0.95);
    long p99 = tracker.getPercentile(0.99);
    assertTrue(p50 >= 100 && p50 <= 300);
    assertTrue(p95 >= 100 && p95 <= 300);
    assertTrue(p99 >= 100 && p99 <= 300);
  }

  @Test
  void testGetPercentileWithZeroSamplesReturnsZero() {
    LatencyTracker tracker = new LatencyTracker(GROUP, PREFIX);
    assertEquals(0, tracker.getPercentile(0.50));
    assertEquals(0, tracker.getPercentile(0.95));
  }

  // Integration-style tests: real Hadoop Context and Counters (no mocks).
  // Uses ReducerContextWrapper to drive a reducer that emits latency counters.

  @Test
  void testEmitCountAndSumOnlyUpdatesJobCounters() throws IOException, InterruptedException {
    Configuration conf = NutchConfiguration.create();
    Map<Text, Text> out = new HashMap<>();
    EmitCountAndSumOnlyReducer reducer = new EmitCountAndSumOnlyReducer(GROUP, PREFIX);
    ReducerContextWrapper<Text, Text, Text, Text> wrapper =
        new ReducerContextWrapper<>(reducer, conf, out);
    reducer.reduce(new Text("k"), Collections.singletonList(new Text("v")), wrapper.getContext());

    LatencyTestUtil.assertCountAndSum(wrapper.getCounters(), GROUP, PREFIX, 2, 30);
  }

  @Test
  void testEmitCountersUpdatesJobCounters() throws IOException, InterruptedException {
    Configuration conf = NutchConfiguration.create();
    Map<Text, Text> out = new HashMap<>();
    EmitCountersReducer reducer = new EmitCountersReducer(GROUP, PREFIX);
    ReducerContextWrapper<Text, Text, Text, Text> wrapper =
        new ReducerContextWrapper<>(reducer, conf, out);
    reducer.reduce(new Text("k"), Collections.singletonList(new Text("v")), wrapper.getContext());

    LatencyTestUtil.assertCountAndSum(wrapper.getCounters(), GROUP, PREFIX, 3, 600);
    LatencyTestUtil.assertPercentilesInRange(wrapper.getCounters(), GROUP, PREFIX, 100, 300);
  }

  @Test
  void testEmitCountersWithZeroSamplesSetsPercentilesToZero() throws IOException, InterruptedException {
    Configuration conf = NutchConfiguration.create();
    Map<Text, Text> out = new HashMap<>();
    EmitCountersZeroReducer reducer = new EmitCountersZeroReducer(GROUP, PREFIX);
    ReducerContextWrapper<Text, Text, Text, Text> wrapper =
        new ReducerContextWrapper<>(reducer, conf, out);
    reducer.reduce(new Text("k"), Collections.emptyList(), wrapper.getContext());

    LatencyTestUtil.assertCountSumAndPercentilesZero(wrapper.getCounters(), GROUP, PREFIX);
  }

  @Test
  void testSetJobLevelCountersUpdatesJobCounters() throws IOException, InterruptedException {
    Configuration conf = NutchConfiguration.create();
    Map<Text, Text> out = new HashMap<>();
    SetJobLevelCountersReducer reducer = new SetJobLevelCountersReducer(GROUP, PREFIX);
    ReducerContextWrapper<Text, Text, Text, Text> wrapper =
        new ReducerContextWrapper<>(reducer, conf, out);
    reducer.reduce(new Text("k"), Collections.singletonList(new Text("v")), wrapper.getContext());

    LatencyTestUtil.assertCountAndSum(wrapper.getCounters(), GROUP, PREFIX, 3, 600);
    LatencyTestUtil.assertPercentilesInRange(wrapper.getCounters(), GROUP, PREFIX, 100, 300);
  }

  @Test
  void testSetJobLevelCountersWithZeroCountSetsPercentilesToZero() throws IOException, InterruptedException {
    Configuration conf = NutchConfiguration.create();
    Map<Text, Text> out = new HashMap<>();
    SetJobLevelCountersZeroReducer reducer = new SetJobLevelCountersZeroReducer(GROUP, PREFIX);
    ReducerContextWrapper<Text, Text, Text, Text> wrapper =
        new ReducerContextWrapper<>(reducer, conf, out);
    reducer.reduce(new Text("k"), Collections.emptyList(), wrapper.getContext());

    LatencyTestUtil.assertCountSumAndPercentilesZero(wrapper.getCounters(), GROUP, PREFIX);
  }

  /** Reducer that emits only count and sum via LatencyTracker (real Context, no mocks). */
  private static final class EmitCountAndSumOnlyReducer extends Reducer<Text, Text, Text, Text> {
    private final String group;
    private final String prefix;

    EmitCountAndSumOnlyReducer(String group, String prefix) {
      this.group = group;
      this.prefix = prefix;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      LatencyTracker tracker = new LatencyTracker(group, prefix);
      tracker.record(10);
      tracker.record(20);
      tracker.emitCountAndSumOnly(context);
    }
  }

  /** Reducer that emits count, sum, and percentiles via LatencyTracker (real Context, no mocks). */
  private static final class EmitCountersReducer extends Reducer<Text, Text, Text, Text> {
    private final String group;
    private final String prefix;

    EmitCountersReducer(String group, String prefix) {
      this.group = group;
      this.prefix = prefix;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      LatencyTracker tracker = new LatencyTracker(group, prefix);
      tracker.record(100);
      tracker.record(200);
      tracker.record(300);
      tracker.emitCounters(context);
    }
  }

  /** Reducer that emits counters with zero samples (percentiles set to 0). */
  private static final class EmitCountersZeroReducer extends Reducer<Text, Text, Text, Text> {
    private final String group;
    private final String prefix;

    EmitCountersZeroReducer(String group, String prefix) {
      this.group = group;
      this.prefix = prefix;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      LatencyTracker tracker = new LatencyTracker(group, prefix);
      tracker.emitCounters(context);
    }
  }

  /** Reducer that calls setJobLevelCounters with a merged digest (real Context, no mocks). */
  private static final class SetJobLevelCountersReducer extends Reducer<Text, Text, Text, Text> {
    private final String group;
    private final String prefix;

    SetJobLevelCountersReducer(String group, String prefix) {
      this.group = group;
      this.prefix = prefix;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      MergingDigest digest = (MergingDigest) TDigest.createMergingDigest(100.0);
      digest.add(100);
      digest.add(200);
      digest.add(300);
      LatencyTracker.setJobLevelCounters(context, group, prefix, 3, 600, digest);
    }
  }

  /** Reducer that calls setJobLevelCounters with zero count (percentiles set to 0). */
  private static final class SetJobLevelCountersZeroReducer extends Reducer<Text, Text, Text, Text> {
    private final String group;
    private final String prefix;

    SetJobLevelCountersZeroReducer(String group, String prefix) {
      this.group = group;
      this.prefix = prefix;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      LatencyTracker.setJobLevelCounters(context, group, prefix, 0, 0, null);
    }
  }
}
