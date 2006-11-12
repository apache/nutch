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
package org.apache.nutch.searcher.url;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.search.BooleanQuery;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class TestURLQueryFilter extends TestCase {

  public TestURLQueryFilter(String name) {
    super(name);
  }

  public void testURLQueryFilter() {
    Configuration conf = NutchConfiguration.create();
    URLQueryFilter urlQueryFilter = new URLQueryFilter();
    urlQueryFilter.setConf(conf);
    try {
      Query q = Query.parse("url:www.apache.org", conf);
      BooleanQuery result = new BooleanQuery();
      urlQueryFilter.filter(q, result);
    } catch (Exception e) {
      fail("Should not throw any exception!");
    }
  }
}
