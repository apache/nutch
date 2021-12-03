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
    conf.set("urlnormalizer.protocols.file", protocolsFile);
    ProtocolURLNormalizer normalizer = new ProtocolURLNormalizer();
    normalizer.setConf(conf);

    // No change
    assertEquals("http://example.org/", normalizer
        .normalize("https://example.org/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.net/", normalizer
        .normalize("https://example.net/", URLNormalizers.SCOPE_DEFAULT));

    // https to http
    assertEquals("http://example.org/", normalizer
        .normalize("https://example.org/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://example.net/", normalizer
        .normalize("https://example.net/", URLNormalizers.SCOPE_DEFAULT));

    // no change
    assertEquals("https://example.io/", normalizer
        .normalize("https://example.io/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("https://example.nl/", normalizer
        .normalize("https://example.nl/", URLNormalizers.SCOPE_DEFAULT));
    
    // http to https
    assertEquals("https://example.io/", normalizer
        .normalize("http://example.io/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("https://example.nl/", normalizer
        .normalize("http://example.nl/", URLNormalizers.SCOPE_DEFAULT));

    // verify proper (de)serialization of URLs
    assertEquals("https://example.io/path?q=uery", normalizer.normalize(
        "http://example.io/path?q=uery", URLNormalizers.SCOPE_DEFAULT));

    // verify that URLs including a port are left unchanged (port and protocol
    // are kept)
    assertEquals("http://example.io:8080/path?q=uery", normalizer.normalize(
        "http://example.io:8080/path?q=uery", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("https://example.org:8443/path", normalizer.normalize(
        "https://example.org:8443/path", URLNormalizers.SCOPE_DEFAULT));

    // verify normalization of all subdomains (host pattern *.example.com)
    assertEquals("https://example.com/", normalizer
        .normalize("http://example.com/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("https://www.example.com/", normalizer
        .normalize("http://www.example.com/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("https://www.subdomain.example.com/", normalizer.normalize(
        "http://www.subdomain.example.com/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://myexample.com/", normalizer
        .normalize("http://myexample.com/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://www.subdomain.example.com:8080/path?q=uery",
        normalizer.normalize(
            "http://www.subdomain.example.com:8080/path?q=uery",
            URLNormalizers.SCOPE_DEFAULT));

    // No change because of invalid rules in protocols.txt
    // (verify that these rules are skipped)
    assertEquals("http://invalid-rule3.example.top/", normalizer
        .normalize("http://invalid-rule3.example.top/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://invalid-rule2.example.top/", normalizer
        .normalize("http://invalid-rule2.example.top/", URLNormalizers.SCOPE_DEFAULT));
    assertEquals("http://invalid-rule3.example.top/", normalizer
        .normalize("http://invalid-rule3.example.top/", URLNormalizers.SCOPE_DEFAULT));
  }
}
