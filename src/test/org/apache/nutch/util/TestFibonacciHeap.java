/**
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

package org.apache.nutch.util;

import junit.framework.TestCase;

import java.util.Arrays;

/** Unit tests for FibonacciHeap. */
public class TestFibonacciHeap extends TestCase {
  public TestFibonacciHeap(String name) { 
    super(name); 
  }

  
  private static class TestItem implements Comparable {
    int id;
    int priority;

    public TestItem(int id, int priority) {
      this.id= id;
      this.priority= priority;
    }

    public String toString() {
      return "<"+id+","+priority+">";
    }

    public int compareTo(Object other) {
      TestItem o= (TestItem) other;
      if (this.priority < o.priority)
        return -1;
      else if (this.priority == o.priority)
        return 0;
      else return 1;
    }
  }

  private final static int NUM_TEST_ITEMS= 200;

  private final static int NUM_TEST_OPERATIONS= 10000;

  // likelihood of doing any of these operations
  private final static double ADD_PROB= .35;
  private final static double DECREASEKEY_PROB= .25;
  private final static double POP_PROB= .30;
  private final static double PEEK_PROB= .10;

  public void testFibHeap() {
    FibonacciHeap h= new FibonacciHeap();

    TestItem[] vals= new TestItem[NUM_TEST_ITEMS];
    for (int i= 0; i < NUM_TEST_ITEMS; i++) 
      vals[i]= new TestItem(i,i);

    // the number of vals in the heap
    int numInVal= 0;
    // the number of vals that are not in the heap
    int numOutVal= NUM_TEST_ITEMS;

    // thresholds
    double addMaxP= ADD_PROB;
    double decreaseKeyMaxP= ADD_PROB + DECREASEKEY_PROB;
    double popMaxP= ADD_PROB + DECREASEKEY_PROB + POP_PROB;

    // number of operations we've done
    int numOps= 0;

    // test add/peek/pop/decreaseKey
    while (numOps < NUM_TEST_OPERATIONS) {

      numOps++;

      assertTrue("heap reports wrong size!", numInVal == h.size());

      double randVal= Math.random();
      if (randVal < addMaxP) {

        if (numOutVal == 0) // can't add...
          continue;

        // add
        int index= ( (NUM_TEST_ITEMS - 1) - 
                     (int) (Math.random() * (double) numOutVal) );
        TestItem tmp= vals[index];
        vals[index]= vals[numInVal];
        vals[numInVal]= tmp;
        numInVal++;
        numOutVal--;

        h.add(tmp, tmp.priority);

      } else if (randVal < decreaseKeyMaxP) {

        // decreaseKey
        if (numInVal == 0) {
          // do nothing
        } else {
          int index= (int) (Math.random() * (double) numInVal);
          TestItem tmp= vals[index];

          tmp.priority-=  Math.random() * 5.0;

          h.decreaseKey(tmp, tmp.priority);
        }

      } else if (randVal < popMaxP) {

        // pop
        if (numInVal == 0) {
          if (h.size() != 0) {
            assertTrue("heap empty, but peekMin() did not return null!",
                       h.peekMin() == null);
            assertTrue("heap empty, but popMin() did not return null!",
                       h.popMin() == null );
          } 
        } else {
          Arrays.sort(vals, 0, numInVal);
          int i= 0; 
          TestItem tmp= (TestItem) h.popMin();
          while ( (i < numInVal) && (tmp.priority == vals[i].priority) ) {
            if (tmp.id == vals[i].id) 
              break;
            i++;
          } 
          assertTrue("popMin did not return lowest-priority item!", 
                     tmp.id == vals[i].id);
          assertTrue("popMin did not return lowest-priority item!",
                     tmp == vals[i]);

          vals[i]= vals[numInVal - 1];
          vals[numInVal - 1]= tmp;
          numInVal--;
          numOutVal++;
        }                               

      } else {

        // peek 
        if (numInVal == 0) {
          assertTrue("heap reports non-zero size when empty", h.size() == 0);
          assertTrue("heap.peekMin() returns item when empty", 
                     h.peekMin() == null);
          assertTrue("heap.popMin() returns item when empty",
                     h.popMin() == null);
        } else {
          Arrays.sort(vals, 0, numInVal);
          int i= 0; 
          TestItem tmp= (TestItem) h.peekMin();

          while ( (i < numInVal) && (tmp.priority == vals[i].priority) ) {
            if (tmp.id == vals[i].id) 
              break;
            i++;
          } 
          assertTrue("heap.peekMin() returns wrong item",
                     tmp.id == vals[i].id);
          assertTrue("heap.peekMin() returns wrong item",
                     tmp == vals[i]);
        }                               
      }
    }

  }

}
