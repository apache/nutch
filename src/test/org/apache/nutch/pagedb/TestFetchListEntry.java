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

package org.apache.nutch.pagedb;

import java.io.*;
import org.apache.nutch.io.*;
import org.apache.nutch.pagedb.*;
import junit.framework.TestCase;

/** Unit tests for FetchListEntry. */
public class TestFetchListEntry extends TestCase {
  public TestFetchListEntry(String name) { super(name); }

  public void testFetchListEntry() throws Exception {

    String[] anchors = new String[] {"foo", "bar"};

    FetchListEntry e1 = new FetchListEntry(true, TestPage.getTestPage(),
                                           anchors);
    TestWritable.testWritable(e1);

    FetchListEntry e2 = new FetchListEntry(false, TestPage.getTestPage(),
                                           anchors);
    TestWritable.testWritable(e2);

  }
	
}
