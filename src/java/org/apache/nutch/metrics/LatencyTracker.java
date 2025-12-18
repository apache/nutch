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

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.tdunning.math.stats.TDigest;

/**
 * A utility class for tracking latency metrics using TDigest for percentile
 * calculation.
 * 
 * <p>This class wraps a TDigest data structure to collect latency samples and
 * emit Hadoop counters with count, sum, and percentile values (p50, p95, p99).
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

  private final TDigest digest;
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
    this.digest = TDigest.createDigest(DEFAULT_COMPRESSION);
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
   * Emits all latency counters to the Hadoop context.
   * 
   * <p>Should be called once during cleanup to emit aggregated metrics.
   * 
   * @param context the Hadoop task context
   */
  public void emitCounters(TaskInputOutputContext<?, ?, ?, ?> context) {
    context.getCounter(group, prefix + "_count_total").setValue(count);
    context.getCounter(group, prefix + "_sum_ms").setValue(sum);
    
    if (count > 0) {
      context.getCounter(group, prefix + "_p50_ms").setValue((long) digest.quantile(0.50));
      context.getCounter(group, prefix + "_p95_ms").setValue((long) digest.quantile(0.95));
      context.getCounter(group, prefix + "_p99_ms").setValue((long) digest.quantile(0.99));
    } else {
      // Set to 0 if no samples recorded
      context.getCounter(group, prefix + "_p50_ms").setValue(0);
      context.getCounter(group, prefix + "_p95_ms").setValue(0);
      context.getCounter(group, prefix + "_p99_ms").setValue(0);
    }
  }
}


