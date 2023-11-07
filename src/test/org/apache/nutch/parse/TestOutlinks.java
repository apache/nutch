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
package org.apache.nutch.parse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("org.apache.nutch.parse")
@Tag("core")
public class TestOutlinks {

  @Test
  public void testAddSameObject() throws Exception {
    Set<Outlink> set = new HashSet<>();

    Outlink o = new Outlink("http://www.example.com", "Example");
    set.add(o);
    set.add(o);

    assertEquals(1, set.size(), "Adding the same Outlink twice");
  }

  @Test
  public void testAddOtherObjectWithSameData() throws Exception {
    Set<Outlink> set = new HashSet<>();

    Outlink o = new Outlink("http://www.example.com", "Example");
    Outlink o1 = new Outlink("http://www.example.com", "Example");

    assertTrue(o.equals(o1), "The two Outlink objects are the same");

    set.add(o);
    set.add(o1);

    assertEquals(1, set.size(), "The set should contain only 1 Outlink");
  }
}
