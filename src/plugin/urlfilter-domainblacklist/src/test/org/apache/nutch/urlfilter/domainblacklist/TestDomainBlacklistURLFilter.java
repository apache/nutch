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
package org.apache.nutch.urlfilter.domainblacklist;

import org.junit.Assert;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

public class TestDomainBlacklistURLFilter {

  private final static String SEPARATOR = System.getProperty("file.separator");
  private final static String SAMPLES = System.getProperty("test.data", ".");

  @Test
  public void testFilter()
    throws Exception {

    String domainBlacklistFile = SAMPLES + SEPARATOR + "hosts.txt";
    Configuration conf = NutchConfiguration.create();
    DomainBlacklistURLFilter domainBlacklistFilter = new DomainBlacklistURLFilter(domainBlacklistFile);
    domainBlacklistFilter.setConf(conf);
    Assert.assertNull(domainBlacklistFilter.filter("http://lucene.apache.org"));
    Assert.assertNull(domainBlacklistFilter.filter("http://hadoop.apache.org"));
    Assert.assertNull(domainBlacklistFilter.filter("http://www.apache.org"));
    Assert.assertNotNull(domainBlacklistFilter.filter("http://www.google.com"));
    Assert.assertNotNull(domainBlacklistFilter.filter("http://mail.yahoo.com"));
    Assert.assertNull(domainBlacklistFilter.filter("http://www.foobar.net"));
    Assert.assertNull(domainBlacklistFilter.filter("http://www.foobas.net"));
    Assert.assertNull(domainBlacklistFilter.filter("http://www.yahoo.com"));
    Assert.assertNull(domainBlacklistFilter.filter("http://www.foobar.be"));
    Assert.assertNotNull(domainBlacklistFilter.filter("http://www.adobe.com"));
  }

}
