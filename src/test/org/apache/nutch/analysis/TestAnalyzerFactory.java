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
package org.apache.nutch.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

/**
 * Simple test case to verify AnalyzerFactory functionality
 */
public class TestAnalyzerFactory extends TestCase {

  private Configuration conf;
  private AnalyzerFactory factory;
  
  protected void setUp() throws Exception {
      conf = NutchConfiguration.create();
      conf.set("plugin.includes", ".*");
      factory=AnalyzerFactory.get(conf);
  }
  
  public void testGetNull() {
    NutchAnalyzer analyzer=factory.get((String)null);
    assertSame(analyzer, factory.getDefault());
  }

  public void testGetExisting() {
    NutchAnalyzer analyzer=factory.get("en");
    assertNotNull(analyzer);
  }

  public void testGetNonExisting() {
    NutchAnalyzer analyzer=factory.get("imaginary-non-existing-language");
    assertSame(analyzer, factory.getDefault());
  }

  public void testCaching() {
    NutchAnalyzer analyzer1=factory.get("en");
    NutchAnalyzer analyzer2=factory.get("en");
    assertEquals(analyzer1, analyzer2);
  }

}
