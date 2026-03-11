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

import java.nio.ByteBuffer;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

/**
 * A utility class for tracking latency metrics using TDigest for percentile
 * calculation.
 *
 * <p>This class wraps a MergingDigest data structure to collect latency samples and
 * emit Hadoop counters with count, sum, and percentile values (p50, p95, p99).
 * MergingDigest supports merging digests from multiple tasks for job-level percentile
 * computation.
 *
 * <p>Usage:
 * <pre>
 * // In mapper/reducer setup
 * latencyTracker = new LatencyTracker(NutchMetrics.GROUP_FETCHER, NutchMetrics.FETCHER_LATENCY);
 *
 * // During processing
 * long start = System.currentTimeMillis();
 * // ... operation ...
 * latencyTracker.record(System.currentTimeMillis() - start);
 *
 * // In cleanup
 * latencyTracker.emitCounters(context);
 * </pre>
 *
 * <p>Emits the following counters:
 * <ul>
 *   <li>{prefix}_count_total - total number of samples</li>
 *   <li>{prefix}_sum_ms - sum of all latencies in milliseconds</li>
 *   <li>{prefix}_p50_ms - 50th percentile (median) latency</li>
 *   <li>{prefix}_p95_ms - 95th percentile latency</li>
 *   <li>{prefix}_p99_ms - 99th percentile latency</li>
 * </ul>
 *
 * @since 1.22
 */
public class LatencyTracker {

  /** Default compression factor for TDigest (controls accuracy vs memory). */
  private static final double DEFAULT_COMPRESSION = 100.0;

  /** Counter name suffix for total sample count. */
  public static final String SUFFIX_COUNT_TOTAL = "_count_total";
  /** Counter name suffix for sum of latencies in milliseconds. */
  public static final String SUFFIX_SUM_MS = "_sum_ms";
  /** Counter name suffix for 50th percentile latency in milliseconds. */
  public static final String SUFFIX_P50_MS = "_p50_ms";
  /** Counter name suffix for 95th percentile latency in milliseconds. */
  public static final String SUFFIX_P95_MS = "_p95_ms";
  /** Counter name suffix for 99th percentile latency in milliseconds. */
  public static final String SUFFIX_P99_MS = "_p99_ms";

  private final MergingDigest digest;
  private final String group;
  private final String prefix;
  private long count = 0;
  private long sum = 0;

  /**
   * Creates a new LatencyTracker.
   *
   * @param group the Hadoop counter group name
   * @param prefix the prefix for counter names (e.g., "fetch_latency")
   */
  public LatencyTracker(String group, String prefix) {
    this.digest = (MergingDigest) TDigest.createMergingDigest(DEFAULT_COMPRESSION);
    this.group = group;
    this.prefix = prefix;
  }

  /**
   * Records a latency sample.
   *
   * @param latencyMs the latency in milliseconds
   */
  public void record(long latencyMs) {
    digest.add(latencyMs);
    count++;
    sum += latencyMs;
  }

  /**
   * Merges another LatencyTracker's digest and aggregates count/sum into this one.
   * Used to combine per-thread or per-task metrics before emitting or serializing.
   *
   * @param other the other tracker to merge in (not modified)
   */
  public void merge(LatencyTracker other) {
    if (other == null || other.count == 0) {
      return;
    }
    digest.add(other.digest);
    count += other.count;
    sum += other.sum;
  }

  /**
   * Returns the number of recorded samples.
   *
   * @return the count of recorded latency samples
   */
  public long getCount() {
    return count;
  }

  /**
   * Returns the sum of all recorded latencies.
   *
   * @return the sum of latencies in milliseconds
   */
  public long getSum() {
    return sum;
  }

  /**
   * Returns the percentile value for the given quantile.
   *
   * @param quantile the quantile (0.0 to 1.0)
   * @return the percentile value in milliseconds
   */
  public long getPercentile(double quantile) {
    if (count == 0) {
      return 0;
    }
    return (long) digest.quantile(quantile);
  }

  /**
   * Serializes the digest to bytes for transmission to a reducer or side file.
   * Returns an empty array if no samples have been recorded.
   *
   * @return serialized digest bytes, or empty array if count is 0
   */
  public byte[] toBytes() {
    if (count == 0) {
      return new byte[0];
    }
    ByteBuffer buf = ByteBuffer.allocate(digest.smallByteSize());
    digest.asSmallBytes(buf);
    return buf.array();
  }

  /**
   * Deserializes a MergingDigest from bytes (as produced by {@link #toBytes()}).
   *
   * @param bytes serialized digest bytes
   * @return MergingDigest instance, or null if bytes is null or empty
   */
  public static MergingDigest fromBytes(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return null;
    }
    return MergingDigest.fromBytes(ByteBuffer.wrap(bytes));
  }

  /**
   * Emits only count and sum counters (not percentiles). Use in mappers when
   * a reducer will merge TDigests and set job-level percentile counters.
   */
  public void emitCountAndSumOnly(TaskInputOutputContext<?, ?, ?, ?> context) {
    context.getCounter(group, prefix + SUFFIX_COUNT_TOTAL).setValue(count);
    context.getCounter(group, prefix + SUFFIX_SUM_MS).setValue(sum);
  }

  /**
   * Emits all latency counters to the Hadoop context.
   *
   * <p>Should be called once during cleanup to emit aggregated metrics.
   *
   * @param context the Hadoop task context
   */
  public void emitCounters(TaskInputOutputContext<?, ?, ?, ?> context) {
    context.getCounter(group, prefix + SUFFIX_COUNT_TOTAL).setValue(count);
    context.getCounter(group, prefix + SUFFIX_SUM_MS).setValue(sum);

    if (count > 0) {
      context.getCounter(group, prefix + SUFFIX_P50_MS).setValue((long) digest.quantile(0.50));
      context.getCounter(group, prefix + SUFFIX_P95_MS).setValue((long) digest.quantile(0.95));
      context.getCounter(group, prefix + SUFFIX_P99_MS).setValue((long) digest.quantile(0.99));
    } else {
      // Set to 0 if no samples recorded
      context.getCounter(group, prefix + SUFFIX_P50_MS).setValue(0);
      context.getCounter(group, prefix + SUFFIX_P95_MS).setValue(0);
      context.getCounter(group, prefix + SUFFIX_P99_MS).setValue(0);
    }
  }

  /**
   * Sets job-level percentile counters from a merged digest (e.g. in a reducer
   * that merged TDigests from all tasks). Uses the same counter names as
   * {@link #emitCounters(TaskInputOutputContext)}.
   *
   * @param context the Hadoop task context
   * @param mergedCount total count from merged digest
   * @param mergedSum total sum from merged digest
   * @param mergedDigest the merged MergingDigest (may be null if mergedCount is 0)
   */
  public static void setJobLevelCounters(TaskInputOutputContext<?, ?, ?, ?> context,
      String group, String prefix, long mergedCount, long mergedSum, MergingDigest mergedDigest) {
    context.getCounter(group, prefix + SUFFIX_COUNT_TOTAL).setValue(mergedCount);
    context.getCounter(group, prefix + SUFFIX_SUM_MS).setValue(mergedSum);
    if (mergedCount > 0 && mergedDigest != null) {
      context.getCounter(group, prefix + SUFFIX_P50_MS).setValue((long) mergedDigest.quantile(0.50));
      context.getCounter(group, prefix + SUFFIX_P95_MS).setValue((long) mergedDigest.quantile(0.95));
      context.getCounter(group, prefix + SUFFIX_P99_MS).setValue((long) mergedDigest.quantile(0.99));
    } else {
      context.getCounter(group, prefix + SUFFIX_P50_MS).setValue(0);
      context.getCounter(group, prefix + SUFFIX_P95_MS).setValue(0);
      context.getCounter(group, prefix + SUFFIX_P99_MS).setValue(0);
    }
  }
}
