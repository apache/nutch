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

package org.apache.nutch.indexer.basic;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import static org.junit.Assert.*;

public class TestBasicDuplicateFilter {
  
  private static Configuration conf = NutchConfiguration.create();
  private static BasicDuplicateFilter filter = new BasicDuplicateFilter();
  private static List<CharSequence> duplicates;
  private static CharSequence[] urls;
  
  @BeforeClass
  public static void setup() {
    filter.setConf(conf);
    
    duplicates = new ArrayList<>();
    urls = new CharSequence[] {
      "http://localhost:8080/page/one",
      "http://localhost:8080/pages/page/page1.html",
      "http://localhost:8080/pages/page/page1.html?var=true",
      "http://localhost:8080/pages/1",
      "http://localhost:8080/pages/page/one",
    };
    for (CharSequence url : urls) {
      duplicates.add(url);
    }
  }
  
  @Test
  public void testShortestIsOriginal() throws Throwable {
    assertEquals(filter.filter(duplicates, null), urls[3]);
  }
}
