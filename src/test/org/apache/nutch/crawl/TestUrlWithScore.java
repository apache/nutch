/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.crawl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.io.RawComparator;
import org.apache.nutch.crawl.UrlWithScore;
import org.apache.nutch.crawl.UrlWithScore.UrlOnlyPartitioner;
import org.apache.nutch.crawl.UrlWithScore.UrlScoreComparator;
import org.apache.nutch.crawl.UrlWithScore.UrlScoreComparator.UrlOnlyComparator;

/**
 * Tests {@link UrlWithScore} with serialization, partitioning and sorting.
 */
public class TestUrlWithScore {

  @Test
  public void testSerialization() throws IOException {
    // create a key and test basic functionality
    UrlWithScore keyOut = new UrlWithScore("http://example.org/", 1f);
    assertEquals("http://example.org/", keyOut.getUrl().toString());
    assertEquals(1f, keyOut.getScore().get(), 0.001);
    
    // write to out
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bos);
    keyOut.write(out);

    // read from in
    UrlWithScore keyIn = new UrlWithScore();
    ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
    DataInputStream in = new DataInputStream(bis);
    keyIn.readFields(in);
    assertEquals(keyOut.getUrl().toString(), keyIn.getUrl().toString());
    assertEquals(keyOut.getScore().get(), keyIn.getScore().get(), 0.001);

    in.close();
    out.close();
  }
  
  @Test
  public void testPartitioner() throws IOException {
    UrlOnlyPartitioner part = new UrlOnlyPartitioner();
    
    UrlWithScore k1 = new UrlWithScore("http://example.org/1", 1f);
    UrlWithScore k2 = new UrlWithScore("http://example.org/1", 2f);
    UrlWithScore k3 = new UrlWithScore("http://example.org/2", 1f);
    UrlWithScore k4 = new UrlWithScore("http://example.org/2", 2f);
    UrlWithScore k5 = new UrlWithScore("http://example.org/2", 3f);
    
    int numReduces = 7;
    
    // keys 1 and 2 should be partitioned together
    int partForKey1 = part.getPartition(k1, null, numReduces);
    assertEquals(partForKey1, part.getPartition(k2, null, numReduces));
    assertEquals(partForKey1, part.getPartition(k2, null, numReduces));
    
    // keys 3, 4 and 5 should be partitioned together
    int partForKey3 = part.getPartition(k3, null, numReduces);
    assertEquals(partForKey3, part.getPartition(k4, null, numReduces));
    assertEquals(partForKey3, part.getPartition(k5, null, numReduces));
  }
  
  @Test
  public void testUrlOnlySorting() throws IOException {
    UrlOnlyComparator comp = new UrlOnlyComparator();
    
    UrlWithScore k1 = new UrlWithScore("http://example.org/1", 1f);
    UrlWithScore k2 = new UrlWithScore("http://example.org/1", 2f);
    UrlWithScore k3 = new UrlWithScore("http://example.org/2", 1f);
    UrlWithScore k4 = new UrlWithScore("http://example.org/2", 2f);
    UrlWithScore k5 = new UrlWithScore("http://example.org/2", 3f);
    
    // k1 should be equal to k2
    assertEquals(0, compareBothRegularAndRaw(comp, k1, k2));
    // test symmetry
    assertEquals(0, compareBothRegularAndRaw(comp, k2, k1));
    
    // k1 is before k3, k4 and k5
    assertEquals(-1, compareBothRegularAndRaw(comp, k1, k3));
    assertEquals(-1, compareBothRegularAndRaw(comp, k1, k4));
    assertEquals(-1, compareBothRegularAndRaw(comp, k1, k5));
    // test symmetry
    assertEquals(1, compareBothRegularAndRaw(comp, k3, k1));
    assertEquals(1, compareBothRegularAndRaw(comp, k4, k1));
    assertEquals(1, compareBothRegularAndRaw(comp, k5, k1));
  }
  
  @Test
  public void testUrlScoreSorting() throws IOException {
    UrlScoreComparator comp = new UrlScoreComparator();
    
    UrlWithScore k1 = new UrlWithScore("http://example.org/1", 1f);
    UrlWithScore k2 = new UrlWithScore("http://example.org/1", 2f);
    UrlWithScore k3 = new UrlWithScore("http://example.org/2", 1f);
    UrlWithScore k4 = new UrlWithScore("http://example.org/2", 2f);
    UrlWithScore k5 = new UrlWithScore("http://example.org/2", 3f);
    
    // k1 is after k2, because score is lower
    assertEquals(1, comp.compare(k1, k2));
    // test symmetry
    assertEquals(-1, comp.compare(k2, k1));
    
    // k1 is before k3, k4 and k5, because url is lower
    assertEquals(-1, compareBothRegularAndRaw(comp, k1, k3));
    assertEquals(-1, compareBothRegularAndRaw(comp, k1, k4));
    assertEquals(-1, compareBothRegularAndRaw(comp, k1, k5));
    // test symmetry
    assertEquals(1, compareBothRegularAndRaw(comp, k3, k1));
    assertEquals(1, compareBothRegularAndRaw(comp, k4, k1));
    assertEquals(1, compareBothRegularAndRaw(comp, k5, k1));
    
    // k3 after k4 and k4 after k5 and therefore k3 after k5 (transitivity)
    assertEquals(1, compareBothRegularAndRaw(comp, k3, k4));
    assertEquals(1, compareBothRegularAndRaw(comp, k4, k5));
    assertEquals(1, compareBothRegularAndRaw(comp, k3, k5));
    // test symmetry
    assertEquals(-1, compareBothRegularAndRaw(comp, k4, k3));
    assertEquals(-1, compareBothRegularAndRaw(comp, k5, k4));
    assertEquals(-1, compareBothRegularAndRaw(comp, k5, k3));
  }

  /**
   * Compares two keys using both regular and raw comparing. Checks if the two
   * values are equal. Returns the compare value, but only when equal.
   * 
   * @param comp
   * @param k1
   * @param k2
   * @return The compare result. (When k1 != k2, assert failure kicks in)
   * @throws IOException 
   */
  private Object compareBothRegularAndRaw(RawComparator<UrlWithScore> comp, 
      UrlWithScore k1, UrlWithScore k2) throws IOException {
    int regular = comp.compare(k1, k2);
    
    byte[] bytes1 = extractBytes(k1);
    byte[] bytes2 = extractBytes(k2);
    
    int raw = comp.compare(bytes1, 0, bytes1.length, bytes2, 0, bytes2.length);
    
    assertEquals("Regular compare should equal raw compare", regular, raw);
    
    return regular;
  }

  /**
   * Return the bytes for a key.
   * 
   * @param k
   * @return
   * @throws IOException
   */
  private byte[] extractBytes(UrlWithScore k) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bos);
    k.write(out);
    byte[] bytes = bos.toByteArray();
    out.close();
    return bytes;
  }
  
}
