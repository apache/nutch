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

package org.apache.nutch.fetcher;

import java.io.*;
import org.apache.nutch.io.*;
import org.apache.nutch.pagedb.*;
import org.apache.nutch.protocol.ProtocolStatus;

import junit.framework.TestCase;

/** Unit tests for FetcherOutput. */
public class TestFetcherOutput extends TestCase {
  public TestFetcherOutput(String name) { super(name); }

  public void testFetcherOutput() throws Exception {

    String[] anchors = new String[] {"foo", "bar"};

    FetcherOutput o =
      new FetcherOutput(new FetchListEntry(true, TestPage.getTestPage(),
                                           anchors),
                        TestMD5Hash.getTestHash(), ProtocolStatus.STATUS_SUCCESS);
                        
    TestWritable.testWritable(o);

  }
	
}
