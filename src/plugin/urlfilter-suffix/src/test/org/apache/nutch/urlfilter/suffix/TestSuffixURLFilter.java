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
package org.apache.nutch.urlfilter.suffix;

import java.io.IOException;
import java.io.StringReader;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;


/**
 * JUnit test for <code>SuffixURLFilter</code>.
 *
 * @author Andrzej Bialecki
 */
public class TestSuffixURLFilter {
  private static final String suffixes =
    "# this is a comment\n" +
    "\n" +
    ".gif\n" +
    ".jpg\n" +
    ".js\n";
  
  private static final String[] urls = new String[] {
    "http://www.example.com/test.gif",
    "http://www.example.com/TEST.GIF",
    "http://www.example.com/test.jpg",
    "http://www.example.com/test.JPG",
    "http://www.example.com/test.html",
    "http://www.example.com/test.HTML",
    "http://www.example.com/test.html?q=abc.js",
    "http://www.example.com/test.js?foo=bar&baz=bar#12333",
  };
  
  private static String[] urlsModeAccept = new String[] {
    null,
    urls[1],
    null,
    urls[3],
    urls[4],
    urls[5],
    null,
    urls[7]
  };
  
  private static String[] urlsModeReject = new String[] {
    urls[0],
    null,
    urls[2],
    null,
    null,
    null,
    urls[6],
    null
  };
  
  private static String[] urlsModeAcceptIgnoreCase = new String[] {
    null,
    null,
    null,
    null,
    urls[4],
    urls[5],
    null,
    urls[7]
  };
 
  private static String[] urlsModeRejectIgnoreCase = new String[] {
    urls[0],
    urls[1],
    urls[2],
    urls[3],
    null,
    null,
    urls[6],
    null
  };
  
  private static String[] urlsModeAcceptAndPathFilter = new String[] {
    null,
    urls[1],
    null,
    urls[3],
    urls[4],
    urls[5],
    urls[6],
    null
  };
  
  private static String[] urlsModeAcceptAndNonPathFilter = new String[] {
    null,
    urls[1],
    null,
    urls[3],
    urls[4],
    urls[5],
    null,
    urls[7]
  };
  
  private SuffixURLFilter filter = null;
  
  @Before
  public void setUp() throws IOException {
    filter = new SuffixURLFilter(new StringReader(suffixes));
  }
  
  @Test
  public void testModeAccept() {
    filter.setIgnoreCase(false);
    filter.setModeAccept(true);
    for (int i = 0; i < urls.length; i++) {
      assertTrue(urlsModeAccept[i] == filter.filter(urls[i]));
    }
  }

  @Test
  public void testModeReject() {
    filter.setIgnoreCase(false);
    filter.setModeAccept(false);
    for (int i = 0; i < urls.length; i++) {
      assertTrue(urlsModeReject[i] == filter.filter(urls[i]));
    }
  }

  @Test
  public void testModeAcceptIgnoreCase() {
    filter.setIgnoreCase(true);
    filter.setModeAccept(true);
    for (int i = 0; i < urls.length; i++) {
      assertTrue(urlsModeAcceptIgnoreCase[i] == filter.filter(urls[i]));
    }
  }

  @Test
  public void testModeRejectIgnoreCase() {
    filter.setIgnoreCase(true);
    filter.setModeAccept(false);
    for (int i = 0; i < urls.length; i++) {
      assertTrue(urlsModeRejectIgnoreCase[i] == filter.filter(urls[i]));
    }
  }
  
  @Test
  public void testModeAcceptAndNonPathFilter() {
    filter.setModeAccept(true);
    filter.setFilterFromPath(false);
    for (int i = 0; i < urls.length; i++) {
      assertTrue(urlsModeAcceptAndNonPathFilter[i] == filter.filter(urls[i]));
    }
  }
  
  @Test
  public void testModeAcceptAndPathFilter() {
    filter.setModeAccept(true);
    filter.setFilterFromPath(true);
    for (int i = 0; i < urls.length; i++) {
      assertTrue(urlsModeAcceptAndPathFilter[i] == filter.filter(urls[i]));
    }
  }

}
