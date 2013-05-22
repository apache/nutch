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

package org.apache.nutch.net.urlnormalizer.regex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.NutchConfiguration;

/** Unit tests for RegexUrlNormalizer. */
public class TestRegexURLNormalizer {
  private static final Logger LOG = LoggerFactory.getLogger(TestRegexURLNormalizer.class);
  
  private RegexURLNormalizer normalizer;
  private Configuration conf;
  private HashMap<String, NormalizedURL[]> testData = new HashMap<String, NormalizedURL[]>();
  
  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data", ".");
  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/urlnormalizer-regex/build.xml during plugin compilation.
  
  @Before
  public void setUp() throws IOException {
    normalizer = new RegexURLNormalizer();
    conf = NutchConfiguration.create();
    normalizer.setConf(conf);
    File[] configs = new File(sampleDir).listFiles(new FileFilter() {
      public boolean accept(File f) {
        if (f.getName().endsWith(".xml") && f.getName().startsWith("regex-normalize-"))
          return true;
        return false;
      }
    });
    for (int i = 0; i < configs.length; i++) {
      try {
        FileReader reader = new FileReader(configs[i]);
        String cname = configs[i].getName();
        cname = cname.substring(16, cname.indexOf(".xml"));
        normalizer.setConfiguration(reader, cname);
        NormalizedURL[] urls = readTestFile(cname);
        testData.put(cname, urls);
      } catch (Exception e) {
        LOG.warn("Could load config from '" + configs[i] + "': " + e.toString());
      }
    }
  }

  @Test
  public void testNormalizerDefault() throws Exception {
    normalizeTest((NormalizedURL[])testData.get(URLNormalizers.SCOPE_DEFAULT),
            URLNormalizers.SCOPE_DEFAULT);
  }

  @Test
  public void testNormalizerScope() throws Exception {
    Iterator<String> it = testData.keySet().iterator();
    while (it.hasNext()) {
      String scope = it.next();
      normalizeTest((NormalizedURL[])testData.get(scope), scope);
    }
  }

  private void normalizeTest(NormalizedURL[] urls, String scope) throws Exception {
    for (int i = 0; i < urls.length; i++) {
      String url = urls[i].url;
      String normalized = normalizer.normalize(urls[i].url, scope);
      String expected = urls[i].expectedURL;
      LOG.info("scope: " + scope + " url: " + url + " | normalized: " + normalized + " | expected: " + expected);
      assertEquals(urls[i].expectedURL, normalized);
    }
  }
	
  /** Currently this is not being used in this class
  private void bench(int loops, String scope) {
    long start = System.currentTimeMillis();
    try {
      NormalizedURL[] expected = (NormalizedURL[])testData.get(scope);
      if (expected == null) return;
      for (int i = 0; i < loops; i++) {
        normalizeTest(expected, scope);
      }
    } catch (Exception e) {
      fail(e.toString());
    }
    LOG.info("bench time (" + loops + ") " +
             (System.currentTimeMillis() - start) + "ms");
  }
  */

  private static class NormalizedURL {
    String url;
    String expectedURL;

    public NormalizedURL(String line) {
      String[] fields = line.split("\\s+");
      url = fields[0];
      expectedURL = fields[1];
    }
  }

  private NormalizedURL[] readTestFile(String scope) throws IOException {
    File f = new File(sampleDir, "regex-normalize-" + scope + ".test");
    BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(f), "UTF-8"));
    List<NormalizedURL> list = new ArrayList<NormalizedURL>();
    String line;
    while((line = in.readLine()) != null) {
      if (  line.trim().length() == 0 ||
            line.startsWith("#") ||
            line.startsWith(" ")) continue;
      list.add(new NormalizedURL(line));
    }
    in.close();
    return (NormalizedURL[]) list.toArray(new NormalizedURL[list.size()]);
  }  
}
