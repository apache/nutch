/*
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

public class TestUrlNormalizerFactory extends TestCase {

  public void testGetNormalizer() {
    Configuration conf=NutchConfiguration.create();
    
    UrlNormalizerFactory factory=new UrlNormalizerFactory(conf);
    
    UrlNormalizer normalizer1=factory.getNormalizer();
    UrlNormalizer normalizer2=factory.getNormalizer();
    assertNotNull(normalizer1);
    assertEquals(normalizer1, normalizer2);
  }
}
