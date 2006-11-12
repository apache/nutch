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

package org.apache.nutch.searcher;

import org.apache.hadoop.io.*;
import junit.framework.TestCase;

public class TestHitDetails extends TestCase {
  public TestHitDetails(String name) { super(name); }

  public void testHitDetails() throws Exception {
    final int length = 4;
    final String[] fields = new String[] {"a", "b", "c", "a" };
    final String[] values = new String[] { "foo1", "bar", "baz", "foo2" };

    HitDetails before = new HitDetails(fields, values);

    DataOutputBuffer dob = new DataOutputBuffer();
    before.write(dob);

    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), dob.getLength());

    HitDetails after = HitDetails.read(dib);

    assertEquals(length, after.getLength());
    for (int i = 0; i < 3; i++) {
      assertEquals(fields[i], after.getField(i));
      assertEquals(values[i], after.getValue(i));
      assertEquals(values[i], after.getValue(fields[i]));
    }
    String[] vals = after.getValues("a");
    assertEquals(2, vals.length);
    assertEquals("foo1", vals[0]);
    assertEquals("foo2", vals[1]);
    vals = after.getValues("b");
    assertEquals(1, vals.length);
    assertEquals("bar", vals[0]);
    vals = after.getValues("c");
    assertEquals(1, vals.length);
    assertEquals("baz", vals[0]);
  }
}
