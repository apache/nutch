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
package org.apache.nutch.net.urlnormalizer.host;

import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class TestHostURLNormalizer extends TestCase {

  private final static String SEPARATOR = System.getProperty("file.separator");
  private final static String SAMPLES = System.getProperty("test.data", ".");

  public void testHostURLNormalizer() throws Exception {
    Configuration conf = NutchConfiguration.create();

    String hostsFile = SAMPLES + SEPARATOR + "hosts.txt";
    HostURLNormalizer normalizer = new HostURLNormalizer(hostsFile);
    normalizer.setConf(conf);

    // Force www. sub domain when hitting link without sub domain
    assertEquals("http://www.example.org/page.html", normalizer.normalize("http://example.org/page.html", URLNormalizers.SCOPE_DEFAULT));

    // Force no sub domain to www. URL's
    assertEquals("http://example.net/path/to/something.html", normalizer.normalize("http://www.example.net/path/to/something.html", URLNormalizers.SCOPE_DEFAULT));

    // Force all sub domains to www.
    assertEquals("http://example.com/?does=it&still=work", normalizer.normalize("http://example.com/?does=it&still=work", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.com/buh", normalizer.normalize("http://http.www.example.com/buh", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.com/blaat", normalizer.normalize("http://whatever.example.com/blaat", URLNormalizers.SCOPE_DEFAULT));
  }
}
