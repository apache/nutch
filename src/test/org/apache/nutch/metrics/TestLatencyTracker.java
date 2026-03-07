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

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link LatencyTracker} merge, serialization (toBytes/fromBytes),
 * and percentile behavior.
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
}
