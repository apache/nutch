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

import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestURLNormalizers {

  @Test
  public void testURLNormalizers() {
    Configuration conf = NutchConfiguration.create();
    String clazz1 = "org.apache.nutch.net.urlnormalizer.regex.RegexURLNormalizer";
    String clazz2 = "org.apache.nutch.net.urlnormalizer.basic.BasicURLNormalizer";
    conf.set("urlnormalizer.order", clazz1 + " " + clazz2);
    
    URLNormalizers normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_DEFAULT);
    
    assertNotNull(normalizers);
    try {
      normalizers.normalize("http://www.example.com/", URLNormalizers.SCOPE_DEFAULT);
    } catch (MalformedURLException mue) {
      fail(mue.toString());
    }

    // NUTCH-1011 - Get rid of superfluous slashes
    try {
      String normalizedSlashes = normalizers.normalize("http://www.example.org//path/to//somewhere.html", URLNormalizers.SCOPE_DEFAULT);
      assertEquals(normalizedSlashes, "http://www.example.org/path/to/somewhere.html");
    } catch (MalformedURLException mue) {
      fail(mue.toString());
    }

    // check the order
    int pos1 = -1, pos2 = -1;
    URLNormalizer[] impls = normalizers.getURLNormalizers(URLNormalizers.SCOPE_DEFAULT);
    for (int i = 0; i < impls.length; i++) {
      if (impls[i].getClass().getName().equals(clazz1)) pos1 = i;
      if (impls[i].getClass().getName().equals(clazz2)) pos2 = i;
    }
    if (pos1 != -1 && pos2 != -1) {
      assertTrue("RegexURLNormalizer before BasicURLNormalizer", pos1 < pos2);
    }
  }
}
