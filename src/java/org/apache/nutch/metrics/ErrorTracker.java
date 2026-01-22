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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * A utility class for tracking errors by category with automatic classification.
 * 
 * <p>This class provides thread-safe error counting with automatic categorization
 * based on exception type. It uses a bounded set of error categories to stay within
 * Hadoop's counter limits (~120 counters).
 * 
 * <p>Usage:
 * <pre>
 * // In mapper/reducer setup or thread initialization
 * errorTracker = new ErrorTracker(NutchMetrics.GROUP_FETCHER);
 * 
 * // When catching exceptions
 * try {
 *     // ... operation ...
 * } catch (Exception e) {
 *     errorTracker.recordError(e);  // Auto-categorizes
 * }
 * 
 * // Or with manual categorization
 * errorTracker.recordError(ErrorTracker.ErrorType.NETWORK);
 * 
 * // In cleanup - emit all error counters
 * errorTracker.emitCounters(context);
 * </pre>
 * 
 * <p>Emits the following counters:
 * <ul>
 *   <li>errors_total - total number of errors across all categories</li>
 *   <li>errors_network_total - network-related errors</li>
 *   <li>errors_protocol_total - protocol errors</li>
 *   <li>errors_parsing_total - parsing errors</li>
 *   <li>errors_url_total - URL-related errors</li>
 *   <li>errors_scoring_total - scoring filter errors</li>
 *   <li>errors_indexing_total - indexing filter errors</li>
 *   <li>errors_timeout_total - timeout errors</li>
 *   <li>errors_other_total - uncategorized errors</li>
 * </ul>
 * 
 * @since 1.22
 */
public class ErrorTracker {

  /**
   * Error type categories for classification.
   * Uses a bounded set to stay within Hadoop's counter limits.
   */
  public enum ErrorType {
    /** Network-related errors (IOException, SocketException, etc.) */
    NETWORK,
    /** Protocol errors (ProtocolException, ProtocolNotFound) */
    PROTOCOL,
    /** Parsing errors (ParseException, ParserNotFound) */
    PARSING,
    /** URL-related errors (MalformedURLException, URLFilterException) */
    URL,
    /** Scoring filter errors */
    SCORING,
    /** Indexing filter errors */
    INDEXING,
    /** Timeout errors (SocketTimeoutException) */
    TIMEOUT,
    /** Other uncategorized errors */
    OTHER
  }

  private final String group;
  private final Map<ErrorType, AtomicLong> counts;
  private final AtomicLong totalCount;
  
  // Cached counter references for performance (optional - set via initCounters)
  private org.apache.hadoop.mapreduce.Counter cachedTotalCounter;
  private final Map<ErrorType, org.apache.hadoop.mapreduce.Counter> cachedCounters;

  /**
   * Creates a new ErrorTracker for the specified counter group.
   * 
   * <p>This constructor creates an ErrorTracker without cached counters.
   * Call {@link #initCounters(TaskInputOutputContext)} in setup() to cache
   * counter references for better performance.
   * 
   * @param group the Hadoop counter group name (e.g., NutchMetrics.GROUP_FETCHER)
   */
  public ErrorTracker(String group) {
    this.group = group;
    this.counts = new EnumMap<>(ErrorType.class);
    this.cachedCounters = new EnumMap<>(ErrorType.class);
    this.totalCount = new AtomicLong(0);
    
    // Initialize all counts to 0
    for (ErrorType type : ErrorType.values()) {
      counts.put(type, new AtomicLong(0));
    }
  }

  /**
   * Creates a new ErrorTracker with cached counter references.
   * 
   * <p>This constructor caches all counter references at creation time,
   * avoiding repeated counter lookups in hot paths.
   * 
   * @param group the Hadoop counter group name
   * @param context the Hadoop task context for caching counters
   */
  public ErrorTracker(String group, TaskInputOutputContext<?, ?, ?, ?> context) {
    this(group);
    initCounters(context);
  }

  /**
   * Initializes cached counter references from the Hadoop context.
   * 
   * <p>Call this method in the mapper/reducer setup() method to cache
   * counter references and avoid repeated lookups during processing.
   * 
   * @param context the Hadoop task context
   */
  public void initCounters(TaskInputOutputContext<?, ?, ?, ?> context) {
    cachedTotalCounter = context.getCounter(group, NutchMetrics.ERROR_TOTAL);
    for (ErrorType type : ErrorType.values()) {
      cachedCounters.put(type, context.getCounter(group, getCounterName(type)));
    }
  }

  /**
   * Records an error with automatic categorization based on the throwable type.
   * 
   * @param t the throwable to categorize and record
   */
  public void recordError(Throwable t) {
    recordError(categorize(t));
  }

  /**
   * Records an error with explicit category.
   * 
   * @param type the error type category
   */
  public void recordError(ErrorType type) {
    counts.get(type).incrementAndGet();
    totalCount.incrementAndGet();
  }

  /**
   * Returns the count for a specific error type.
   * 
   * @param type the error type
   * @return the count for that error type
   */
  public long getCount(ErrorType type) {
    return counts.get(type).get();
  }

  /**
   * Returns the total count of all errors.
   * 
   * @return the total error count
   */
  public long getTotalCount() {
    return totalCount.get();
  }

  /**
   * Emits all error counters to the Hadoop context.
   * 
   * <p>Should be called once during cleanup to emit aggregated metrics.
   * Only emits counters for error types that have non-zero counts.
   * 
   * <p>If counters were cached via {@link #initCounters(TaskInputOutputContext)},
   * uses the cached references for better performance.
   * 
   * @param context the Hadoop task context
   */
  public void emitCounters(TaskInputOutputContext<?, ?, ?, ?> context) {
    // Use cached counters if available, otherwise look up
    if (cachedTotalCounter != null) {
      cachedTotalCounter.increment(totalCount.get());
      for (ErrorType type : ErrorType.values()) {
        long count = counts.get(type).get();
        if (count > 0) {
          cachedCounters.get(type).increment(count);
        }
      }
    } else {
      // Fallback to direct lookup
      context.getCounter(group, NutchMetrics.ERROR_TOTAL).increment(totalCount.get());
      for (ErrorType type : ErrorType.values()) {
        long count = counts.get(type).get();
        if (count > 0) {
          context.getCounter(group, getCounterName(type)).increment(count);
        }
      }
    }
  }

  /**
   * Directly increments cached error counters without local accumulation.
   * 
   * <p>Use this method when you want to immediately update Hadoop counters
   * rather than accumulating locally and emitting in cleanup.
   * Requires {@link #initCounters(TaskInputOutputContext)} to have been called.
   * 
   * @param t the throwable to categorize and count
   * @throws IllegalStateException if counters have not been initialized
   */
  public void incrementCounters(Throwable t) {
    incrementCounters(categorize(t));
  }

  /**
   * Directly increments cached error counters without local accumulation.
   * 
   * <p>Use this method when you want to immediately update Hadoop counters
   * rather than accumulating locally and emitting in cleanup.
   * Requires {@link #initCounters(TaskInputOutputContext)} to have been called.
   * 
   * @param type the error type to count
   * @throws IllegalStateException if counters have not been initialized
   */
  public void incrementCounters(ErrorType type) {
    if (cachedTotalCounter == null) {
      throw new IllegalStateException(
          "Counters not initialized. Call initCounters() first.");
    }
    cachedTotalCounter.increment(1);
    cachedCounters.get(type).increment(1);
  }

  /**
   * Categorizes a throwable into an error type.
   * 
   * <p>The categorization checks the exception class hierarchy to determine
   * the most appropriate category. Timeout exceptions are checked first as
   * they are a subclass of IOException.
   * 
   * @param t the throwable to categorize
   * @return the appropriate ErrorType for the throwable
   */
  public static ErrorType categorize(Throwable t) {
    if (t == null) {
      return ErrorType.OTHER;
    }
    
    String className = t.getClass().getName();
    
    // Check for timeout first (before general IOException)
    if (t instanceof SocketTimeoutException 
        || className.contains("TimeoutException")
        || className.contains("Timeout")) {
      return ErrorType.TIMEOUT;
    }
    
    // Network errors
    if (t instanceof SocketException 
        || t instanceof UnknownHostException
        || className.contains("ConnectException")
        || className.contains("NoRouteToHostException")
        || className.contains("ConnectionRefusedException")) {
      return ErrorType.NETWORK;
    }
    
    // URL errors (check before general IOException since MalformedURLException extends IOException)
    if (t instanceof MalformedURLException
        || className.contains("URLFilterException")
        || className.contains("URISyntaxException")) {
      return ErrorType.URL;
    }
    
    // General IOException (but not the specific subtypes above)
    if (t instanceof IOException) {
      return ErrorType.NETWORK;
    }
    
    // Protocol errors
    if (className.contains("ProtocolException")
        || className.contains("ProtocolNotFound")) {
      return ErrorType.PROTOCOL;
    }
    
    // Parsing errors
    if (className.contains("ParseException")
        || className.contains("ParserNotFound")
        || className.contains("SAXException")
        || className.contains("ParserConfigurationException")) {
      return ErrorType.PARSING;
    }
    
    // Scoring errors
    if (className.contains("ScoringFilterException")) {
      return ErrorType.SCORING;
    }
    
    // Indexing errors
    if (className.contains("IndexingException")) {
      return ErrorType.INDEXING;
    }
    
    // Check cause chain for more specific categorization
    Throwable cause = t.getCause();
    if (cause != null && cause != t) {
      ErrorType causeType = categorize(cause);
      if (causeType != ErrorType.OTHER) {
        return causeType;
      }
    }
    
    return ErrorType.OTHER;
  }

  /**
   * Gets the counter name constant for a given error type.
   * 
   * @param type the error type
   * @return the counter name constant from NutchMetrics
   */
  public static String getCounterName(ErrorType type) {
    switch (type) {
      case NETWORK:
        return NutchMetrics.ERROR_NETWORK_TOTAL;
      case PROTOCOL:
        return NutchMetrics.ERROR_PROTOCOL_TOTAL;
      case PARSING:
        return NutchMetrics.ERROR_PARSING_TOTAL;
      case URL:
        return NutchMetrics.ERROR_URL_TOTAL;
      case SCORING:
        return NutchMetrics.ERROR_SCORING_TOTAL;
      case INDEXING:
        return NutchMetrics.ERROR_INDEXING_TOTAL;
      case TIMEOUT:
        return NutchMetrics.ERROR_TIMEOUT_TOTAL;
      case OTHER:
      default:
        return NutchMetrics.ERROR_OTHER_TOTAL;
    }
  }

  /**
   * Gets the counter name for a throwable based on its categorization.
   * 
   * <p>This is a convenience method for direct use in catch blocks:
   * <pre>
   * } catch (Exception e) {
   *     context.getCounter(group, ErrorTracker.getCounterName(e)).increment(1);
   * }
   * </pre>
   * 
   * @param t the throwable to get the counter name for
   * @return the counter name constant from NutchMetrics
   */
  public static String getCounterName(Throwable t) {
    return getCounterName(categorize(t));
  }
}
