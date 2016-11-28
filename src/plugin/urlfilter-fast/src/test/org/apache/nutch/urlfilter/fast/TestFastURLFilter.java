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
package org.apache.nutch.urlfilter.fast;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.apache.nutch.net.URLFilter;
import org.apache.nutch.urlfilter.api.RegexURLFilterBaseTest;
import org.junit.Assert;
import org.junit.Test;


public class TestFastURLFilter extends RegexURLFilterBaseTest {

  protected URLFilter getURLFilter(Reader rules) {
    try {
      return new FastURLFilter(rules);
    } catch (IOException e) {
      Assert.fail(e.toString());
      return null;
    }
  }

  @Test
  public void test() {
    test("fast-urlfilter-test.txt", "test.urls");
    test("fast-urlfilter-benchmark.txt", "Benchmarks.urls");
  }

  @Test
  public void benchmark() {
    bench(50, "fast-urlfilter-benchmark.txt", "Benchmarks.urls");
    bench(100, "fast-urlfilter-benchmark.txt", "Benchmarks.urls");
    bench(200, "fast-urlfilter-benchmark.txt", "Benchmarks.urls");
    bench(400, "fast-urlfilter-benchmark.txt", "Benchmarks.urls");
    bench(800, "fast-urlfilter-benchmark.txt", "Benchmarks.urls");
  }

}
