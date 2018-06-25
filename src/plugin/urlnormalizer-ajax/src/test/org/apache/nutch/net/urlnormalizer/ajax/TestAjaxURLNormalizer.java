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

package org.apache.nutch.net.urlnormalizer.ajax;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

/** Unit tests for AjaxURLNormalizer. */
public class TestAjaxURLNormalizer extends TestCase {
  private AjaxURLNormalizer normalizer;
  private Configuration conf;
  
  public TestAjaxURLNormalizer(String name) {
    super(name);
    normalizer = new AjaxURLNormalizer();
    conf = NutchConfiguration.create();
    normalizer.setConf(conf);
  }

  public void testNormalizer() throws Exception {
    // check if AJAX URL's are normalized to an _escaped_frament_ form
    normalizeTest("http://example.org/#!k=v", "http://example.org/?_escaped_fragment_=k=v");

    // Check with some escaped chars
    normalizeTest("http://example.org/#!k=v&something=is wrong", "http://example.org/?_escaped_fragment_=k=v%26something=is%20wrong");

    // Check with query string and multiple fragment params
    normalizeTest("http://example.org/path.html?queryparam=queryvalue#!key1=value1&key2=value2", "http://example.org/path.html?queryparam=queryvalue&_escaped_fragment_=key1=value1%26key2=value2");
  }
  
  public void testNormalizerWhenIndexing() throws Exception {
    // check if it works the other way around
    normalizeTest("http://example.org/?_escaped_fragment_=key=value", "http://example.org/#!key=value", URLNormalizers.SCOPE_INDEXER);
    normalizeTest("http://example.org/?key=value&_escaped_fragment_=key=value", "http://example.org/?key=value#!key=value", URLNormalizers.SCOPE_INDEXER);
    normalizeTest("http://example.org/page.html?key=value&_escaped_fragment_=key=value%26something=is%20wrong", "http://example.org/page.html?key=value#!key=value&something=is wrong", URLNormalizers.SCOPE_INDEXER);
  }

  private void normalizeTest(String weird, String normal) throws Exception {
    assertEquals(normal, normalizer.normalize(weird, URLNormalizers.SCOPE_DEFAULT));
  }
  
  private void normalizeTest(String weird, String normal, String scope) throws Exception {
    assertEquals(normal, normalizer.normalize(weird, scope));
  }

  public static void main(String[] args) throws Exception {
    new TestAjaxURLNormalizer("test").testNormalizer();
  }
}
