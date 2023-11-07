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
package org.apache.nutch.urlfilter.api;

import java.lang.invoke.MethodHandles;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nutch.net.URLFilter;

/**
 * Unit tests for {@link org.apache.nutch.urlfilter.api.RegexURLFilterBase}
 * 
 * @author J&eacute;r&ocirc;me Charron
 */
@Tag("api")
public abstract class RegexURLFilterBaseTest {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  protected final static String SEPARATOR = System.getProperty("file.separator");
  protected final static String SAMPLES = System.getProperty("test.data", ".");

  protected abstract URLFilter getURLFilter(Reader rules);

  protected void bench(int loops, String file) {
    try {
      bench(loops, new FileReader(SAMPLES + SEPARATOR + file + ".rules"),
          new FileReader(SAMPLES + SEPARATOR + file + ".urls"));
    } catch (Exception e) {
      fail(e.toString());
    }
  }

  protected void bench(int loops, Reader rules, Reader urls) {
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    try {
      URLFilter filter = getURLFilter(rules);
      FilteredURL[] expected = readURLFile(urls);
      for (int i = 0; i < loops; i++) {
        test(filter, expected);
      }
    } catch (Exception e) {
      fail(e.toString());
    }
    stopWatch.stop();
    LOG.info("bench time {} loops {} ms", loops, stopWatch.getTime(TimeUnit.MILLISECONDS));
  }

  protected void bench(int loops, String rulesFile, String urlsFile) {
    try {
      bench(loops, new FileReader(SAMPLES + SEPARATOR + rulesFile),
          new FileReader(SAMPLES + SEPARATOR + urlsFile));
    } catch (Exception e) {
      fail(e.toString());
    }
  }

  protected void test(String rulesFile, String urlsFile) {
    try {
      test(new FileReader(SAMPLES + SEPARATOR + rulesFile),
          new FileReader(SAMPLES + SEPARATOR + urlsFile));
    } catch (Exception e) {
      fail(e.toString());
    }
  }

  protected void test(String file) {
    try {
      test(new FileReader(SAMPLES + SEPARATOR + file + ".rules"),
          new FileReader(SAMPLES + SEPARATOR + file + ".urls"));
    } catch (Exception e) {
      fail(e.toString());
    }
  }

  protected void test(Reader rules, Reader urls) {
    try {
      test(getURLFilter(rules), readURLFile(urls));
    } catch (Exception e) {
      fail(e.toString());
    }
  }

  protected void test(URLFilter filter, FilteredURL[] expected) {
    for (int i = 0; i < expected.length; i++) {
      String result = filter.filter(expected[i].url);
      if (result != null) {
        assertTrue(expected[i].sign, expected[i].url);
      } else {
        assertFalse(expected[i].sign, expected[i].url);
      }
    }
  }

  private static FilteredURL[] readURLFile(Reader reader) throws IOException {
    BufferedReader in = new BufferedReader(reader);
    List<FilteredURL> list = new ArrayList<FilteredURL>();
    String line;
    while ((line = in.readLine()) != null) {
      if (line.length() != 0) {
        list.add(new FilteredURL(line));
      }
    }
    return (FilteredURL[]) list.toArray(new FilteredURL[list.size()]);
  }

  private static class FilteredURL {

    boolean sign;
    String url;

    FilteredURL(String line) {
      switch (line.charAt(0)) {
      case '+':
        sign = true;
        break;
      case '-':
        sign = false;
        break;
      default:
        // Simply ignore...
      }
      url = line.substring(1);
    }
  }

}
