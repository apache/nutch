/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.ArrayList;
import java.util.Iterator;

/** 
 * Unit tests for SoftHashMap.
 */
public class TestSoftHashMap extends TestCase {

  // set to true to get flood of status messages on stderr- useful
  // for seeing when JVM is collecting everything.
  private static final boolean verbose= false;

  // 1kB for int[]
  private static final int TEST_VALUE_ARRAY_SIZE= 1024 / 4; 

  private static final int BASIC_OPS_SIZE= 10;

  private boolean keyHasBeenFinalized;
  private boolean valHasBeenFinalized;

  private class TestKey {
    Integer key;
    boolean notify;

    TestKey(Integer key, boolean notify) {
      this.key= key;
      this.notify= notify;
    }

    protected void finalize() {
      if (notify)
        TestSoftHashMap.this.keyFinalized(key);
    }

    public int hashCode() {
      return key.hashCode();
    }

    public boolean equals(Object o) {
      if (o == null) 
        return false;
      if ( !(o instanceof TestKey) )
        return false;
      TestKey other= (TestKey) o;
      return (this.key.equals(other.key));
    }

    public String toString() {
      return "Key:"+key;
    }

  }

  private class TestValue implements SoftHashMap.FinalizationNotifier {
    int[] val;
    boolean notify;
    ArrayList finalizationListeners;

    TestValue(int key, boolean notify) {
      this.val= new int[TEST_VALUE_ARRAY_SIZE];
      this.val[0]= key;
      this.notify= notify;
      this.finalizationListeners= new ArrayList();
    }

    public void addFinalizationListener(SoftHashMap.FinalizationListener
                                        listener) {
      finalizationListeners.add(listener);
    }

    protected void finalize() {
      if (notify)
        TestSoftHashMap.this.valFinalized(val[0]);
      for (Iterator iter= finalizationListeners.iterator();
           iter.hasNext() ; ) {
        SoftHashMap.FinalizationListener l=
          (SoftHashMap.FinalizationListener) iter.next();
        l.finalizationOccurring();
      }
    }

    boolean isMyKey(int key) {
      return key == val[0];
    }

    public String toString() {
      return "Val:"+val[0];
    }

  }

  public TestSoftHashMap(String name) { 
    super(name); 
  }

  public void testBasicOps() {
    SoftHashMap shm= new SoftHashMap();

    // cache keys & vals  so they don't go away
    TestKey[] keys= new TestKey[BASIC_OPS_SIZE];
    TestValue[] vals= new TestValue[BASIC_OPS_SIZE];

    for (int i= 0; i < BASIC_OPS_SIZE; i++) {
      keys[i]= new TestKey(new Integer(i), false);
      vals[i]= new TestValue(i, false);
      shm.put(keys[i], vals[i]);
    }

    for (int i= 0; i < BASIC_OPS_SIZE; i++) {
      TestValue v= (TestValue) shm.get(new TestKey(new Integer(i), false));
      assertTrue("got back null, expecting value! (key= "+i+")", v != null);
      assertTrue("got back wrong value (isMyKey())!", v.isMyKey(i));
      assertTrue("got back wrong value (!=)!", v == vals[i]);
      assertTrue("contains key doesn't have " + i, 
                 shm.containsKey(new TestKey(new Integer(i), false)));
      assertTrue("isEmpty returns true when it shouldn't",
                 !shm.isEmpty());
    }

    Object removed= shm.remove(
      new TestKey(new Integer(BASIC_OPS_SIZE - 1), false));
    if (verbose) 
      System.err.println("removed: " + removed);
    TestValue v= (TestValue) 
      shm.get(new TestKey(new Integer(BASIC_OPS_SIZE), false));
    assertTrue("got back val after delete!", v == null);

    int size= shm.size();
    assertTrue("got bad value from size(); returned " + size, 
               size == (BASIC_OPS_SIZE - 1) );

    shm.clear();
    assertTrue("isEmpty returns false when it shouldn't",
               shm.isEmpty());
  }

  public void testExpiry() {
    if (verbose) 
      System.err.println("entering testExpiry()");
    SoftHashMap shm= new SoftHashMap();

    valHasBeenFinalized= false;
    keyHasBeenFinalized= false;
    int i= 0;

    try {
      while (!valHasBeenFinalized) {
	if (verbose) 
	  System.err.println("(!v) trying to put " + i);
        shm.put(new TestKey(new Integer(i), true), new TestValue(i, true));
        i++;
	if (verbose) 
	  System.err.println("after adding " + i
			     + " items, size is " + shm.size());
      }
      while (!keyHasBeenFinalized) {
	if (verbose) 
	  System.err.println("(!k) trying to put " + i);
        shm.put(new TestKey(new Integer(i), true), new TestValue(i, true));
        i++;
      }

      // sleep and busy loop to see if JVM goes on collecting stuff...
      if (verbose) 
	System.err.println("sleeping... ");
      Thread.sleep(20 * 1000);
      if (verbose) 
	System.err.println("busy looping...");
      int j;
      for (j= 0; j < 2000000; j++) {
	i+= j;
      }
      if (verbose) 
	System.err.println("done, j=" + j);

    } catch (Exception e)  {
      System.err.println("caught exception");
      e.printStackTrace();
    } finally {
      if (verbose) 
	System.err.println("out of put loops");
    }

  }

  void keyFinalized(Integer key) {
    if (verbose) 
      System.err.println("notified of finalized key: " + key);
    keyHasBeenFinalized= true;
  }

  void valFinalized(int key) {
    if (verbose) 
      System.err.println("notified of finalized value for: " + key);
    valHasBeenFinalized= true;
  }

  public static final void main(String[] a) {
    TestSoftHashMap t= new TestSoftHashMap("test");
    t.testExpiry();
  }

}
