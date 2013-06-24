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
package org.apache.nutch.urlfilter.prefix;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import java.io.IOException;


/**
 * JUnit test for <code>PrefixURLFilter</code>.
 *
 * @author Talat Uyarer
 * @author Cihad Guzel
 */
public class TestPrefixURLFilter extends TestCase {
  private static final String prefixes =
    "# this is a comment\n" +
    "\n" +
    "http://\n" +
    "https://\n" +
    "file://\n" +
    "ftp://\n";

  private static final String[] urls = new String[] {
    "http://www.example.com/",
    "https://www.example.com/",
    "ftp://www.example.com/",
    "file://www.example.com/",
    "abcd://www.example.com/",
    "www.example.com/",
  };

  private static String[] urlsModeAccept = new String[] {
    urls[0],
    urls[1],
    urls[2],
    urls[3],
    null,
    null
  };

  private PrefixURLFilter filter = null;

  public static Test suite() {
    return new TestSuite(TestPrefixURLFilter.class);
  }

  public static void main(String[] args) {
    TestRunner.run(suite());
  }

  public void setUp() throws IOException {
    filter = new PrefixURLFilter(prefixes);
  }

  public void testModeAccept() {
    for (int i = 0; i < urls.length; i++) {
      assertTrue(urlsModeAccept[i] == filter.filter(urls[i]));
    }
  }
}
