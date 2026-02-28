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
package org.apache.nutch.crawl;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.Generator.FloatTextPair;
import org.apache.nutch.crawl.Generator.ScoreHostKeyComparator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for NUTCH-2455: Secondary sorting for efficient HostDb merging.
 * Tests the FloatTextPair composite key and ScoreHostKeyComparator.
 */
public class TestGeneratorSecondarySort {

  private ScoreHostKeyComparator comparator;

  public TestGeneratorSecondarySort() {
    // Default constructor
  }

  @BeforeEach
  public void setUp() {
    comparator = new ScoreHostKeyComparator();
  }

  /**
   * Test FloatTextPair basic functionality.
   */
  @Test
  public void testFloatTextPairBasics() {
    FloatTextPair pair1 = new FloatTextPair(1.5f, "example.com");
    assertEquals(1.5f, pair1.getFirst().get(), 0.001f);
    assertEquals("example.com", pair1.getSecond().toString());

    FloatTextPair pair2 = new FloatTextPair(new FloatWritable(2.0f), new Text("test.org"));
    assertEquals(2.0f, pair2.getFirst().get(), 0.001f);
    assertEquals("test.org", pair2.getSecond().toString());

    // Test default constructor
    FloatTextPair pair3 = new FloatTextPair();
    pair3.setFirst(new FloatWritable(3.0f));
    pair3.setSecond(new Text("another.com"));
    assertEquals(3.0f, pair3.getFirst().get(), 0.001f);
    assertEquals("another.com", pair3.getSecond().toString());
  }

  /**
   * Test FloatTextPair serialization/deserialization.
   */
  @Test
  public void testFloatTextPairSerialization() throws IOException {
    FloatTextPair original = new FloatTextPair(1.5f, "example.com");

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    original.write(out);
    out.close();

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream in = new DataInputStream(bais);
    FloatTextPair deserialized = new FloatTextPair();
    deserialized.readFields(in);
    in.close();

    assertEquals(original.getFirst().get(), deserialized.getFirst().get(), 0.001f);
    assertEquals(original.getSecond().toString(), deserialized.getSecond().toString());
  }

  /**
   * Test FloatTextPair equality and hashCode.
   */
  @Test
  public void testFloatTextPairEquality() {
    FloatTextPair pair1 = new FloatTextPair(1.5f, "example.com");
    FloatTextPair pair2 = new FloatTextPair(1.5f, "example.com");
    FloatTextPair pair3 = new FloatTextPair(2.0f, "example.com");
    FloatTextPair pair4 = new FloatTextPair(1.5f, "other.com");

    assertEquals(pair1, pair2);
    assertEquals(pair1.hashCode(), pair2.hashCode());
    assertNotEquals(pair1, pair3);
    assertNotEquals(pair1, pair4);
  }

  /**
   * Test that HostDb entries (with hostname) sort before CrawlDb entries (empty hostname).
   * This is the core behavior needed for NUTCH-2455.
   */
  @Test
  public void testHostDbEntriesSortBeforeCrawlDbEntries() {
    // HostDb entry: has hostname in second field
    FloatTextPair hostDbEntry = new FloatTextPair(-Float.MAX_VALUE, "example.com");
    
    // CrawlDb entry: empty hostname
    FloatTextPair crawlDbEntry = new FloatTextPair(1.0f, "");

    // HostDb should come before CrawlDb
    assertTrue(comparator.compare(hostDbEntry, crawlDbEntry) < 0,
        "HostDb entry should sort before CrawlDb entry");
    assertTrue(comparator.compare(crawlDbEntry, hostDbEntry) > 0,
        "CrawlDb entry should sort after HostDb entry");
  }

  /**
   * Test that multiple HostDb entries sort by hostname.
   */
  @Test
  public void testHostDbEntriesSortByHostname() {
    FloatTextPair hostA = new FloatTextPair(-Float.MAX_VALUE, "aaa.com");
    FloatTextPair hostB = new FloatTextPair(-Float.MAX_VALUE, "bbb.com");
    FloatTextPair hostC = new FloatTextPair(-Float.MAX_VALUE, "ccc.com");

    assertTrue(comparator.compare(hostA, hostB) < 0,
        "aaa.com should sort before bbb.com");
    assertTrue(comparator.compare(hostB, hostC) < 0,
        "bbb.com should sort before ccc.com");
    assertTrue(comparator.compare(hostA, hostC) < 0,
        "aaa.com should sort before ccc.com");
  }

  /**
   * Test that multiple CrawlDb entries sort by score (descending).
   */
  @Test
  public void testCrawlDbEntriesSortByScoreDescending() {
    FloatTextPair highScore = new FloatTextPair(10.0f, "");
    FloatTextPair medScore = new FloatTextPair(5.0f, "");
    FloatTextPair lowScore = new FloatTextPair(1.0f, "");

    // High score should come first (descending order)
    assertTrue(comparator.compare(highScore, medScore) < 0,
        "High score should sort before medium score");
    assertTrue(comparator.compare(medScore, lowScore) < 0,
        "Medium score should sort before low score");
    assertTrue(comparator.compare(highScore, lowScore) < 0,
        "High score should sort before low score");
  }

  /**
   * Test complete sorting order with mixed HostDb and CrawlDb entries.
   */
  @Test
  public void testCompleteSortingOrder() {
    List<FloatTextPair> entries = new ArrayList<>();
    
    // Add CrawlDb entries with various scores
    entries.add(new FloatTextPair(5.0f, ""));   // CrawlDb medium score
    entries.add(new FloatTextPair(10.0f, ""));  // CrawlDb high score
    entries.add(new FloatTextPair(1.0f, ""));   // CrawlDb low score
    
    // Add HostDb entries
    entries.add(new FloatTextPair(-Float.MAX_VALUE, "bbb.com"));
    entries.add(new FloatTextPair(-Float.MAX_VALUE, "aaa.com"));
    
    // Sort using our comparator
    Collections.sort(entries, (a, b) -> comparator.compare(a, b));
    
    // Verify order: HostDb entries first (sorted by hostname), then CrawlDb entries (by score desc)
    assertEquals("aaa.com", entries.get(0).getSecond().toString(),
        "First should be HostDb entry aaa.com");
    assertEquals("bbb.com", entries.get(1).getSecond().toString(),
        "Second should be HostDb entry bbb.com");
    assertEquals(10.0f, entries.get(2).getFirst().get(), 0.001f,
        "Third should be CrawlDb entry with score 10.0");
    assertEquals(5.0f, entries.get(3).getFirst().get(), 0.001f,
        "Fourth should be CrawlDb entry with score 5.0");
    assertEquals(1.0f, entries.get(4).getFirst().get(), 0.001f,
        "Fifth should be CrawlDb entry with score 1.0");
  }

  /**
   * Test FloatTextPair compareTo method.
   */
  @Test
  public void testFloatTextPairCompareTo() {
    FloatTextPair pair1 = new FloatTextPair(1.0f, "a");
    FloatTextPair pair2 = new FloatTextPair(1.0f, "b");
    FloatTextPair pair3 = new FloatTextPair(2.0f, "a");

    // Different scores - compare by score first
    assertTrue(pair1.compareTo(pair3) < 0);
    assertTrue(pair3.compareTo(pair1) > 0);

    // Same score - compare by text
    assertTrue(pair1.compareTo(pair2) < 0);
    assertTrue(pair2.compareTo(pair1) > 0);

    // Equal
    FloatTextPair pair4 = new FloatTextPair(1.0f, "a");
    assertEquals(0, pair1.compareTo(pair4));
  }

  /**
   * Test toString method.
   */
  @Test
  public void testFloatTextPairToString() {
    FloatTextPair pair = new FloatTextPair(1.5f, "example.com");
    String str = pair.toString();
    assertTrue(str.contains("1.5"), "toString should contain score");
    assertTrue(str.contains("example.com"), "toString should contain hostname");
  }
}
