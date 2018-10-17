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

package org.apache.nutch.parse;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class TestOutlinks {

  @Test
  public void testAddSameObject() throws Exception {
    Set<Outlink> set = new HashSet<>();

    Outlink o = new Outlink("http://www.example.com", "Example");
    set.add(o);
    set.add(o);

    assertEquals("Adding the same Outlink twice", 1, set.size());
  }

  @Test
  public void testAddOtherObjectWithSameData() throws Exception {
    Set<Outlink> set = new HashSet<>();

    Outlink o = new Outlink("http://www.example.com", "Example");
    Outlink o1 = new Outlink("http://www.example.com", "Example");

    assertTrue("The two Outlink objects are the same", o.equals(o1));

    set.add(o);
    set.add(o1);

    assertEquals("The set should contain only 1 Outlink", 1, set.size());
  }
}
