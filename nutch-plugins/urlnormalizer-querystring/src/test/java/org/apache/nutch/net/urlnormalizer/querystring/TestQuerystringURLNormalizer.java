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
package org.apache.nutch.net.urlnormalizer.querystring;

import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class TestQuerystringURLNormalizer extends TestCase {

  public void testQuerystringURLNormalizer() throws Exception {
    Configuration conf = NutchConfiguration.create();

    QuerystringURLNormalizer normalizer = new QuerystringURLNormalizer();
    normalizer.setConf(conf);

    assertEquals("http://example.com/?a=b&c=d", normalizer.normalize(
        "http://example.com/?c=d&a=b", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.com/a/b/c", normalizer.normalize(
        "http://example.com/a/b/c", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.com:1234/a/b/c", normalizer.normalize(
        "http://example.com:1234/a/b/c", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.com:1234/a/b/c#ref", normalizer.normalize(
        "http://example.com:1234/a/b/c#ref", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.com:1234/a/b/c?a=b&c=d#ref",
        normalizer.normalize("http://example.com:1234/a/b/c?c=d&a=b#ref",
            URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.com/?a=b&a=c&c=d", normalizer.normalize(
        "http://example.com/?c=d&a=b&a=c", URLNormalizers.SCOPE_DEFAULT));
  }
}
