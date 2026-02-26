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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Unit tests for ParseOutputFormat. */
class TestParseOutputFormat {

  @Test
  void testGetParseMetaToCrawlDBKeysEmpty() {
    assertArrayEquals(new String[0], ParseOutputFormat.getParseMetaToCrawlDBKeys(""));
    assertArrayEquals(new String[0], ParseOutputFormat.getParseMetaToCrawlDBKeys(null));
  }

  @Test
  void testGetParseMetaToCrawlDBKeysSingle() {
    assertArrayEquals(new String[] { "lang" },
        ParseOutputFormat.getParseMetaToCrawlDBKeys("lang"));
    assertArrayEquals(new String[] { "lang" },
        ParseOutputFormat.getParseMetaToCrawlDBKeys("  lang  "));
  }

  @Test
  void testGetParseMetaToCrawlDBKeysCommaSeparated() {
    assertArrayEquals(new String[] { "a", "b" },
        ParseOutputFormat.getParseMetaToCrawlDBKeys("a,b"));
    assertArrayEquals(new String[] { "a", "b", "c" },
        ParseOutputFormat.getParseMetaToCrawlDBKeys("a,b,c"));
  }

  @Test
  void testGetParseMetaToCrawlDBKeysTrimSpacesAroundCommas() {
    assertArrayEquals(new String[] { "a", "b" },
        ParseOutputFormat.getParseMetaToCrawlDBKeys(" a , b "));
    assertArrayEquals(new String[] { "lang", "Content-Type" },
        ParseOutputFormat.getParseMetaToCrawlDBKeys(" lang , Content-Type "));
  }

  @Test
  void testGetParseMetaToCrawlDBKeysEmptySegmentsFiltered() {
    assertArrayEquals(new String[] { "a", "b" },
        ParseOutputFormat.getParseMetaToCrawlDBKeys("a,,b"));
    assertArrayEquals(new String[] { "a" },
        ParseOutputFormat.getParseMetaToCrawlDBKeys("a,,,"));
    assertArrayEquals(new String[0],
        ParseOutputFormat.getParseMetaToCrawlDBKeys(",  ,  ,"));
  }

  @Test
  void testGetParseMetaToCrawlDBKeysNeverNull() {
    assertNotNull(ParseOutputFormat.getParseMetaToCrawlDBKeys(null));
    assertNotNull(ParseOutputFormat.getParseMetaToCrawlDBKeys(""));
  }
}
