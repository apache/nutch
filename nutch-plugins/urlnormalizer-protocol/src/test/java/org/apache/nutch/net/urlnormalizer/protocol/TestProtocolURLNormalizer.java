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
package org.apache.nutch.net.urlnormalizer.protocol;

import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class TestProtocolURLNormalizer extends TestCase {

  private final static String SEPARATOR = System.getProperty("file.separator");
  private final static String SAMPLES = System.getProperty("test.data", ".");

  public void testProtocolURLNormalizer() throws Exception {
    Configuration conf = NutchConfiguration.create();

    String protocolsFile = SAMPLES + SEPARATOR + "protocols.txt";
    ProtocolURLNormalizer normalizer = new ProtocolURLNormalizer(protocolsFile);
    normalizer.setConf(conf);

    // No change
    assertEquals("http://example.org/", normalizer.normalize("https://example.org/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.net/", normalizer.normalize("https://example.net/", URLNormalizers.SCOPE_DEFAULT));
    
    // https to http
    assertEquals("http://example.org/", normalizer.normalize("https://example.org/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.net/", normalizer.normalize("https://example.net/", URLNormalizers.SCOPE_DEFAULT));
    
    // no change
    assertEquals("https://example.io/", normalizer.normalize("https://example.io/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("https://example.nl/", normalizer.normalize("https://example.nl/", URLNormalizers.SCOPE_DEFAULT));
    
    // http to https
    assertEquals("https://example.io/", normalizer.normalize("http://example.io/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("https://example.nl/", normalizer.normalize("http://example.nl/", URLNormalizers.SCOPE_DEFAULT));
  }
}
