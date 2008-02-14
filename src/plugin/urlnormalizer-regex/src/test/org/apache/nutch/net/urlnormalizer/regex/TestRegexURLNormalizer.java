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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

/** Unit tests for RegexUrlNormalizer. */
public class TestRegexURLNormalizer extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestRegexURLNormalizer.class);
  
  private RegexURLNormalizer normalizer;
  private Configuration conf;
  private HashMap testData = new HashMap();
  
  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data", ".");
  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/urlnormalizer-regex/build.xml during plugin compilation.
  
  public TestRegexURLNormalizer(String name) throws IOException {
    super(name);
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
        FileInputStream fis = new FileInputStream(configs[i]);
        String cname = configs[i].getName();
        cname = cname.substring(16, cname.indexOf(".xml"));
        normalizer.setConfiguration(fis, cname);
        NormalizedURL[] urls = readTestFile(cname);
        testData.put(cname, urls);
      } catch (Exception e) {
        LOG.warn("Could load config from '" + configs[i] + "': " + e.toString());
      }
    }
  }

  public void testNormalizerDefault() throws Exception {
    normalizeTest((NormalizedURL[])testData.get(URLNormalizers.SCOPE_DEFAULT),
            URLNormalizers.SCOPE_DEFAULT);
  }

  public void testNormalizerScope() throws Exception {
    Iterator it = testData.keySet().iterator();
    while (it.hasNext()) {
      String scope = (String)it.next();
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
    List list = new ArrayList();
    String line;
    while((line = in.readLine()) != null) {
      if (  line.trim().length() == 0 ||
            line.startsWith("#") ||
            line.startsWith(" ")) continue;
      list.add(new NormalizedURL(line));
    }
    return (NormalizedURL[]) list.toArray(new NormalizedURL[list.size()]);
  }  

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("TestRegexURLNormalizer [-bench <iter>] <scope>");
      System.exit(-1);
    }
    boolean bench = false;
    int iter = -1;
    String scope = null;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-bench")) {
        bench = true;
        iter = Integer.parseInt(args[++i]);
      } else scope = args[i];
    }
    if (scope == null) {
      System.err.println("Missing required scope name.");
      System.exit(-1);
    }
    if (bench && iter < 0) {
      System.err.println("Invalid number of iterations: " + iter);
      System.exit(-1);
    }
    TestRegexURLNormalizer test = new TestRegexURLNormalizer("test");
    NormalizedURL[] urls = (NormalizedURL[])test.testData.get(scope);
    if (urls == null) {
      LOG.warn("Missing test data for scope '" + scope + "', using default scope.");
      scope = URLNormalizers.SCOPE_DEFAULT;
      urls = (NormalizedURL[])test.testData.get(scope);
    }
    if (bench) {
      test.bench(iter, scope);
    } else {
      test.normalizeTest(urls, scope);
    }
  }



}
