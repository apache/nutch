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
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParserNotFound;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.ProtocolNotFound;
import org.apache.nutch.scoring.ScoringFilterException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.xml.sax.SAXException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import org.apache.nutch.metrics.ErrorTracker.ErrorType;

/**
 * Unit tests for {@link ErrorTracker} categorization, counting, and Hadoop
 * counter integration.
 */
@ExtendWith(MockitoExtension.class)
public class TestErrorTracker {

  @Mock
  private TaskInputOutputContext<?, ?, ?, ?> mockContext;

  @Mock
  private Counter mockCounter;

  @BeforeEach
  void setUp() {
    // Configure mock context to return mock counter for any counter request
    lenient().when(mockContext.getCounter(anyString(), anyString())).thenReturn(mockCounter);
  }

  // =========================================================================
  // Network Error Categorization Tests
  // =========================================================================

  @Test
  public void testCategorizeNetworkErrors() {
    // Test IOException
    assertEquals(ErrorType.NETWORK, 
        ErrorTracker.categorize(new IOException("Connection failed")));
    
    // Test SocketException
    assertEquals(ErrorType.NETWORK, 
        ErrorTracker.categorize(new SocketException("Socket closed")));
    
    // Test UnknownHostException
    assertEquals(ErrorType.NETWORK, 
        ErrorTracker.categorize(new UnknownHostException("example.com")));
    
    // Test ConnectException
    assertEquals(ErrorType.NETWORK,
        ErrorTracker.categorize(new ConnectException("Connection refused")));
  }

  // =========================================================================
  // Timeout Error Categorization Tests
  // =========================================================================

  @Test
  public void testCategorizeTimeoutErrors() {
    // Test SocketTimeoutException
    assertEquals(ErrorType.TIMEOUT, 
        ErrorTracker.categorize(new SocketTimeoutException("Read timed out")));
  }

  @Test
  public void testCategorizeTimeoutByClassName() {
    // Test custom exception with "Timeout" in class name
    // The categorize method checks className.contains("Timeout")
    Exception customTimeout = new CustomTimeoutException("Custom timeout");
    assertEquals(ErrorType.TIMEOUT, ErrorTracker.categorize(customTimeout));
  }

  // Custom exception class for testing class name-based detection
  private static class CustomTimeoutException extends Exception {
    CustomTimeoutException(String message) {
      super(message);
    }
  }

  // =========================================================================
  // URL Error Categorization Tests
  // =========================================================================

  @Test
  public void testCategorizeUrlErrors() {
    // Test MalformedURLException
    assertEquals(ErrorType.URL, 
        ErrorTracker.categorize(new MalformedURLException("Invalid URL")));
    
    // Test URISyntaxException
    assertEquals(ErrorType.URL,
        ErrorTracker.categorize(new URISyntaxException("bad uri", "Invalid syntax")));
  }

  @Test
  public void testCategorizeUrlFilterException() {
    // Test URLFilterException (Nutch-specific)
    assertEquals(ErrorType.URL,
        ErrorTracker.categorize(new URLFilterException("URL filtered")));
  }

  // =========================================================================
  // Protocol Error Categorization Tests
  // =========================================================================

  @Test
  public void testCategorizeProtocolErrors() {
    // Test ProtocolException (Nutch-specific)
    assertEquals(ErrorType.PROTOCOL,
        ErrorTracker.categorize(new ProtocolException("Protocol error")));
    
    // Test ProtocolNotFound (Nutch-specific)
    assertEquals(ErrorType.PROTOCOL,
        ErrorTracker.categorize(new ProtocolNotFound("ftp")));
  }

  // =========================================================================
  // Parsing Error Categorization Tests
  // =========================================================================

  @Test
  public void testCategorizeParsingErrors() {
    // Test ParseException (Nutch-specific)
    assertEquals(ErrorType.PARSING,
        ErrorTracker.categorize(new ParseException("Parse failed")));
    
    // Test ParserNotFound (Nutch-specific)
    assertEquals(ErrorType.PARSING,
        ErrorTracker.categorize(new ParserNotFound("text/unknown")));
    
    // Test SAXException
    assertEquals(ErrorType.PARSING,
        ErrorTracker.categorize(new SAXException("XML parse error")));
  }

  // =========================================================================
  // Scoring Error Categorization Tests
  // =========================================================================

  @Test
  public void testCategorizeScoringErrors() {
    // Test ScoringFilterException (Nutch-specific)
    assertEquals(ErrorType.SCORING,
        ErrorTracker.categorize(new ScoringFilterException("Scoring failed")));
  }

  // =========================================================================
  // Indexing Error Categorization Tests
  // =========================================================================

  @Test
  public void testCategorizeIndexingErrors() {
    // Test IndexingException (Nutch-specific)
    assertEquals(ErrorType.INDEXING,
        ErrorTracker.categorize(new IndexingException("Indexing failed")));
  }

  // =========================================================================
  // Other/Fallback Categorization Tests
  // =========================================================================

  @Test
  public void testCategorizeNullThrowable() {
    // Null should return OTHER
    assertEquals(ErrorType.OTHER, ErrorTracker.categorize(null));
  }

  @Test
  public void testCategorizeGenericException() {
    // Generic Exception should return OTHER
    assertEquals(ErrorType.OTHER, 
        ErrorTracker.categorize(new Exception("Generic error")));
    
    // RuntimeException should return OTHER
    assertEquals(ErrorType.OTHER, 
        ErrorTracker.categorize(new RuntimeException("Runtime error")));
  }

  // =========================================================================
  // Cause Chain Categorization Tests
  // =========================================================================

  @Test
  public void testCategorizeCauseChain() {
    // Exception with a network cause should be categorized as NETWORK
    IOException cause = new IOException("Root cause");
    Exception wrapper = new Exception("Wrapper", cause);
    assertEquals(ErrorType.NETWORK, ErrorTracker.categorize(wrapper));
    
    // Exception with a timeout cause should be categorized as TIMEOUT
    SocketTimeoutException timeoutCause = new SocketTimeoutException("Timeout");
    Exception timeoutWrapper = new Exception("Wrapper", timeoutCause);
    assertEquals(ErrorType.TIMEOUT, ErrorTracker.categorize(timeoutWrapper));
  }

  @Test
  public void testCategorizeNestedCauseChain() {
    // Deep nested cause chain: RuntimeException -> Exception -> IOException
    IOException rootCause = new IOException("Root cause");
    Exception middleWrapper = new Exception("Middle", rootCause);
    RuntimeException outerWrapper = new RuntimeException("Outer", middleWrapper);
    assertEquals(ErrorType.NETWORK, ErrorTracker.categorize(outerWrapper));
    
    // Deep nested with Nutch-specific exception
    ScoringFilterException scoringCause = new ScoringFilterException("Scoring error");
    Exception wrapper1 = new Exception("Wrapper 1", scoringCause);
    Exception wrapper2 = new Exception("Wrapper 2", wrapper1);
    assertEquals(ErrorType.SCORING, ErrorTracker.categorize(wrapper2));
  }

  // =========================================================================
  // Record Error Tests (Local Accumulation)
  // =========================================================================

  @Test
  public void testRecordErrorByType() {
    ErrorTracker tracker = new ErrorTracker(NutchMetrics.GROUP_FETCHER);
    
    // Initially all counts should be 0
    assertEquals(0, tracker.getTotalCount());
    assertEquals(0, tracker.getCount(ErrorType.NETWORK));
    
    // Record a NETWORK error
    tracker.recordError(ErrorType.NETWORK);
    assertEquals(1, tracker.getTotalCount());
    assertEquals(1, tracker.getCount(ErrorType.NETWORK));
    assertEquals(0, tracker.getCount(ErrorType.TIMEOUT));
    
    // Record another NETWORK error
    tracker.recordError(ErrorType.NETWORK);
    assertEquals(2, tracker.getTotalCount());
    assertEquals(2, tracker.getCount(ErrorType.NETWORK));
    
    // Record a TIMEOUT error
    tracker.recordError(ErrorType.TIMEOUT);
    assertEquals(3, tracker.getTotalCount());
    assertEquals(2, tracker.getCount(ErrorType.NETWORK));
    assertEquals(1, tracker.getCount(ErrorType.TIMEOUT));
  }

  @Test
  public void testRecordErrorByThrowable() {
    ErrorTracker tracker = new ErrorTracker(NutchMetrics.GROUP_FETCHER);
    
    // Record an IOException (should be categorized as NETWORK)
    tracker.recordError(new IOException("Test"));
    assertEquals(1, tracker.getTotalCount());
    assertEquals(1, tracker.getCount(ErrorType.NETWORK));
    
    // Record a SocketTimeoutException (should be categorized as TIMEOUT)
    tracker.recordError(new SocketTimeoutException("Test"));
    assertEquals(2, tracker.getTotalCount());
    assertEquals(1, tracker.getCount(ErrorType.TIMEOUT));
    
    // Record a MalformedURLException (should be categorized as URL)
    tracker.recordError(new MalformedURLException("Test"));
    assertEquals(3, tracker.getTotalCount());
    assertEquals(1, tracker.getCount(ErrorType.URL));
  }

  // =========================================================================
  // Counter Name Mapping Tests
  // =========================================================================

  @Test
  public void testGetCounterName() {
    // Test counter name mapping
    assertEquals(NutchMetrics.ERROR_NETWORK_TOTAL, 
        ErrorTracker.getCounterName(ErrorType.NETWORK));
    assertEquals(NutchMetrics.ERROR_PROTOCOL_TOTAL, 
        ErrorTracker.getCounterName(ErrorType.PROTOCOL));
    assertEquals(NutchMetrics.ERROR_PARSING_TOTAL, 
        ErrorTracker.getCounterName(ErrorType.PARSING));
    assertEquals(NutchMetrics.ERROR_URL_TOTAL, 
        ErrorTracker.getCounterName(ErrorType.URL));
    assertEquals(NutchMetrics.ERROR_SCORING_TOTAL, 
        ErrorTracker.getCounterName(ErrorType.SCORING));
    assertEquals(NutchMetrics.ERROR_INDEXING_TOTAL, 
        ErrorTracker.getCounterName(ErrorType.INDEXING));
    assertEquals(NutchMetrics.ERROR_TIMEOUT_TOTAL, 
        ErrorTracker.getCounterName(ErrorType.TIMEOUT));
    assertEquals(NutchMetrics.ERROR_OTHER_TOTAL, 
        ErrorTracker.getCounterName(ErrorType.OTHER));
  }

  @Test
  public void testGetCounterNameForThrowable() {
    // Test getting counter name directly from throwable
    assertEquals(NutchMetrics.ERROR_NETWORK_TOTAL, 
        ErrorTracker.getCounterName(new IOException("Test")));
    assertEquals(NutchMetrics.ERROR_TIMEOUT_TOTAL, 
        ErrorTracker.getCounterName(new SocketTimeoutException("Test")));
    assertEquals(NutchMetrics.ERROR_URL_TOTAL, 
        ErrorTracker.getCounterName(new MalformedURLException("Test")));
    assertEquals(NutchMetrics.ERROR_OTHER_TOTAL, 
        ErrorTracker.getCounterName(new RuntimeException("Test")));
    
    // Test Nutch-specific exceptions
    assertEquals(NutchMetrics.ERROR_PROTOCOL_TOTAL,
        ErrorTracker.getCounterName(new ProtocolException("Test")));
    assertEquals(NutchMetrics.ERROR_PARSING_TOTAL,
        ErrorTracker.getCounterName(new ParseException("Test")));
    assertEquals(NutchMetrics.ERROR_SCORING_TOTAL,
        ErrorTracker.getCounterName(new ScoringFilterException("Test")));
    assertEquals(NutchMetrics.ERROR_INDEXING_TOTAL,
        ErrorTracker.getCounterName(new IndexingException("Test")));
  }

  // =========================================================================
  // Hadoop Context Integration Tests (Using Mocks)
  // =========================================================================

  @Test
  public void testConstructorWithContext() {
    // Create ErrorTracker with context - should initialize counters
    ErrorTracker tracker = new ErrorTracker(NutchMetrics.GROUP_FETCHER, mockContext);
    
    // Verify counters were requested from context
    // Total counter + 8 error type counters = 9 calls
    verify(mockContext, atLeast(9)).getCounter(anyString(), anyString());
  }

  @Test
  public void testInitCounters() {
    ErrorTracker tracker = new ErrorTracker(NutchMetrics.GROUP_FETCHER);
    
    // Initialize counters
    tracker.initCounters(mockContext);
    
    // Verify counters were requested
    verify(mockContext).getCounter(NutchMetrics.GROUP_FETCHER, NutchMetrics.ERROR_TOTAL);
    verify(mockContext).getCounter(NutchMetrics.GROUP_FETCHER, NutchMetrics.ERROR_NETWORK_TOTAL);
    verify(mockContext).getCounter(NutchMetrics.GROUP_FETCHER, NutchMetrics.ERROR_TIMEOUT_TOTAL);
  }

  @Test
  public void testIncrementCountersWithType() {
    ErrorTracker tracker = new ErrorTracker(NutchMetrics.GROUP_FETCHER, mockContext);
    
    // Increment counters directly
    tracker.incrementCounters(ErrorType.NETWORK);
    
    // Verify counter was incremented (total + specific type)
    verify(mockCounter, times(2)).increment(1);
  }

  @Test
  public void testIncrementCountersWithThrowable() {
    ErrorTracker tracker = new ErrorTracker(NutchMetrics.GROUP_FETCHER, mockContext);
    
    // Increment counters with throwable
    tracker.incrementCounters(new IOException("Test"));
    
    // Verify counter was incremented (total + NETWORK type)
    verify(mockCounter, times(2)).increment(1);
  }

  @Test
  public void testIncrementCountersWithoutInit() {
    ErrorTracker tracker = new ErrorTracker(NutchMetrics.GROUP_FETCHER);
    
    // Should throw IllegalStateException when counters not initialized
    assertThrows(IllegalStateException.class, () -> {
      tracker.incrementCounters(ErrorType.NETWORK);
    });
  }

  @Test
  public void testEmitCounters() {
    ErrorTracker tracker = new ErrorTracker(NutchMetrics.GROUP_FETCHER);
    
    // Record some errors locally
    tracker.recordError(ErrorType.NETWORK);
    tracker.recordError(ErrorType.NETWORK);
    tracker.recordError(ErrorType.TIMEOUT);
    
    // Emit counters (without cached counters - uses fallback)
    tracker.emitCounters(mockContext);
    
    // Verify counters were requested and incremented
    verify(mockContext).getCounter(NutchMetrics.GROUP_FETCHER, NutchMetrics.ERROR_TOTAL);
    verify(mockContext).getCounter(NutchMetrics.GROUP_FETCHER, NutchMetrics.ERROR_NETWORK_TOTAL);
    verify(mockContext).getCounter(NutchMetrics.GROUP_FETCHER, NutchMetrics.ERROR_TIMEOUT_TOTAL);
  }

  @Test
  public void testEmitCountersWithCachedCounters() {
    ErrorTracker tracker = new ErrorTracker(NutchMetrics.GROUP_FETCHER, mockContext);
    
    // Reset mock to clear constructor calls
    reset(mockCounter);
    
    // Record some errors locally
    tracker.recordError(ErrorType.NETWORK);
    tracker.recordError(ErrorType.NETWORK);
    tracker.recordError(ErrorType.TIMEOUT);
    
    // Emit counters (with cached counters)
    tracker.emitCounters(mockContext);
    
    // Verify cached counters were used (increment called with accumulated values)
    verify(mockCounter).increment(3L); // total count
    verify(mockCounter).increment(2L); // NETWORK count
    verify(mockCounter).increment(1L); // TIMEOUT count
  }

  // =========================================================================
  // Thread Safety Tests
  // =========================================================================

  @Test
  public void testThreadSafety() throws InterruptedException {
    ErrorTracker tracker = new ErrorTracker(NutchMetrics.GROUP_FETCHER);
    
    // Create multiple threads that record errors concurrently
    Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        for (int j = 0; j < 100; j++) {
          tracker.recordError(ErrorType.NETWORK);
        }
      });
    }
    
    // Start all threads
    for (Thread thread : threads) {
      thread.start();
    }
    
    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }
    
    // Verify counts
    assertEquals(1000, tracker.getTotalCount());
    assertEquals(1000, tracker.getCount(ErrorType.NETWORK));
  }

  @Test
  public void testThreadSafetyMixedErrorTypes() throws InterruptedException {
    ErrorTracker tracker = new ErrorTracker(NutchMetrics.GROUP_FETCHER);
    
    // Create threads that record different error types concurrently
    Thread networkThread = new Thread(() -> {
      for (int i = 0; i < 500; i++) {
        tracker.recordError(ErrorType.NETWORK);
      }
    });
    
    Thread timeoutThread = new Thread(() -> {
      for (int i = 0; i < 300; i++) {
        tracker.recordError(ErrorType.TIMEOUT);
      }
    });
    
    Thread urlThread = new Thread(() -> {
      for (int i = 0; i < 200; i++) {
        tracker.recordError(ErrorType.URL);
      }
    });
    
    networkThread.start();
    timeoutThread.start();
    urlThread.start();
    
    networkThread.join();
    timeoutThread.join();
    urlThread.join();
    
    // Verify counts
    assertEquals(1000, tracker.getTotalCount());
    assertEquals(500, tracker.getCount(ErrorType.NETWORK));
    assertEquals(300, tracker.getCount(ErrorType.TIMEOUT));
    assertEquals(200, tracker.getCount(ErrorType.URL));
  }
}
