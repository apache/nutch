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
package org.apache.nutch.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class TestURLFilters extends TestCase {

  /**
   * Testcase for NUTCH-325.
   * @throws URLFilterException
   */
  public void testNonExistingUrlFilter() throws URLFilterException {
    Configuration conf = NutchConfiguration.create();
    String class1 = "NonExistingFilter";
    String class2 = "org.apache.nutch.urlfilter.prefix.PrefixURLFilter";
    conf.set(URLFilters.URLFILTER_ORDER, class1 + " " + class2);

    URLFilters normalizers = new URLFilters(conf);
    normalizers.filter("http://someurl/");
  }

}
