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
package org.apache.nutch.urlfilter.domain;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestDomainURLFilter {

  private final static String SEPARATOR = System.getProperty("file.separator");
  private final static String SAMPLES = System.getProperty("test.data", ".");

  @Test
  public void testFilter() throws Exception {

    String domainFile = SAMPLES + SEPARATOR + "hosts.txt";
    Configuration conf = NutchConfiguration.create();
    DomainURLFilter domainFilter = new DomainURLFilter(domainFile);
    domainFilter.setConf(conf);
    Assert.assertNotNull(domainFilter.filter("http://lucene.apache.org"));
    Assert.assertNotNull(domainFilter.filter("http://hadoop.apache.org"));
    Assert.assertNotNull(domainFilter.filter("http://www.apache.org"));
    Assert.assertNull(domainFilter.filter("http://www.google.com"));
    Assert.assertNull(domainFilter.filter("http://mail.yahoo.com"));
    Assert.assertNotNull(domainFilter.filter("http://www.foobar.net"));
    Assert.assertNotNull(domainFilter.filter("http://www.foobas.net"));
    Assert.assertNotNull(domainFilter.filter("http://www.yahoo.com"));
    Assert.assertNotNull(domainFilter.filter("http://www.foobar.be"));
    Assert.assertNull(domainFilter.filter("http://www.adobe.com"));
  }
  
  @Test
  public void testNoFilter() throws Exception {
    // https://issues.apache.org/jira/browse/NUTCH-2189
    String domainFile = SAMPLES + SEPARATOR + "this-file-does-not-exist.txt";
    Configuration conf = NutchConfiguration.create();
    DomainURLFilter domainFilter = new DomainURLFilter(domainFile);
    domainFilter.setConf(conf);
    Assert.assertNotNull(domainFilter.filter("http://lucene.apache.org"));
    Assert.assertNotNull(domainFilter.filter("http://hadoop.apache.org"));
    Assert.assertNotNull(domainFilter.filter("http://www.apache.org"));
    Assert.assertNotNull(domainFilter.filter("http://www.google.com"));
    Assert.assertNotNull(domainFilter.filter("http://mail.yahoo.com"));
    Assert.assertNotNull(domainFilter.filter("http://www.foobar.net"));
    Assert.assertNotNull(domainFilter.filter("http://www.foobas.net"));
    Assert.assertNotNull(domainFilter.filter("http://www.yahoo.com"));
    Assert.assertNotNull(domainFilter.filter("http://www.foobar.be"));
    Assert.assertNotNull(domainFilter.filter("http://www.adobe.com"));
  }

}
