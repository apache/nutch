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
package org.apache.nutch.net.urlnormalizer.slash;

import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class TestSlashURLNormalizer extends TestCase {

  private final static String SEPARATOR = System.getProperty("file.separator");
  private final static String SAMPLES = System.getProperty("test.data", ".");

  public void testSlashURLNormalizer() throws Exception {
    Configuration conf = NutchConfiguration.create();

    String slashesFile = SAMPLES + SEPARATOR + "slashes.txt";
    SlashURLNormalizer normalizer = new SlashURLNormalizer(slashesFile);
    normalizer.setConf(conf);

    // No change
    assertEquals("http://example.org/", normalizer.normalize("http://example.org/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.net/", normalizer.normalize("http://example.net/", URLNormalizers.SCOPE_DEFAULT));
    
    // Don't touch base URL's
    assertEquals("http://example.org", normalizer.normalize("http://example.org", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.net", normalizer.normalize("http://example.net", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.org/", normalizer.normalize("http://example.org/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.net/", normalizer.normalize("http://example.net/", URLNormalizers.SCOPE_DEFAULT));
    
    // Change
    assertEquals("http://www.example.org/page/", normalizer.normalize("http://www.example.org/page", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://www.example.net/path/to/something", normalizer.normalize("http://www.example.net/path/to/something/", URLNormalizers.SCOPE_DEFAULT));
    
    // No change
    assertEquals("http://example.org/buh/", normalizer.normalize("http://example.org/buh/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.net/blaat", normalizer.normalize("http://example.net/blaat", URLNormalizers.SCOPE_DEFAULT));
    
    // No change
    assertEquals("http://example.nl/buh/", normalizer.normalize("http://example.nl/buh/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.de/blaat", normalizer.normalize("http://example.de/blaat", URLNormalizers.SCOPE_DEFAULT));
    
    // Change
    assertEquals("http://www.example.org/page/?a=b&c=d", normalizer.normalize("http://www.example.org/page?a=b&c=d", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://www.example.net/path/to/something?a=b&c=d", normalizer.normalize("http://www.example.net/path/to/something/?a=b&c=d", URLNormalizers.SCOPE_DEFAULT));
    
    // No change
    assertEquals("http://www.example.org/noise.mp3", normalizer.normalize("http://www.example.org/noise.mp3", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://www.example.org/page.html", normalizer.normalize("http://www.example.org/page.html", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://www.example.org/page.shtml", normalizer.normalize("http://www.example.org/page.shtml", URLNormalizers.SCOPE_DEFAULT));

    // Change
    assertEquals("http://www.example.org/this.is.not.an_extension/", normalizer.normalize("http://www.example.org/this.is.not.an_extension", URLNormalizers.SCOPE_DEFAULT));
  }
}
