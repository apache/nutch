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
package org.apache.nutch.parse.js;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class JSParseFilterTest extends TestCase {

  static Configuration conf = NutchConfiguration.create();

  public void testExtractUrlFromEmbeddedHTML() {
    String js = "// embedded html\n"
        + "html = \"<a href=\\\"http://www.example.com\\\">example</a>";
    assertFound(js, "http://www.example.com", "http://www.example.com/");
  }

  public void testExtractUrlFromParameterValue() {
    String js = "// parameter\n" + "url = \"http://www.example.com\"";
    assertFound(js, "http://www.example.com", "http://www.example.com/");
  }

  public void testExtractUrlFromFunctionParameter() {
    String js = "load('http://www.example.com/');return false;";
    assertFound(js, "http://www.example.com", "http://www.example.com/");

    js = "load(\"http://www.example.com/\");";
    assertFound(js, "http://www.example.com", "http://www.example.com/");

    js = "load(\"http://www.example.com/\");";
    assertFound(js, "http://www.example.com", "http://www.example.com/");
  }

  public void testExtractUrlMissingProtocol() {
    String js = "// parameter\n" + "url = \"www.example.com\"";
    assertFound(js, "http://www.example.com", "http://www.example.com/");
  }

  public void testExtractUrlServerRelative() {
    String js = "// parameter\n" + "url = \"/page.html\"";
    assertFound(js, "http://www.example.com/foo/bar/",
        "http://www.example.com/page.html");
  }

  public void testExtractUrlRelative() {
    String js = "// parameter\n" + "url = \"page.html\"";
    assertFound(js, "http://www.example.com",
        "http://www.example.com/page.html");
  }

  private void assertFound(String js, String base, String expected) {
    System.out.println("jS:" + js);
    JSParseFilter filter = new JSParseFilter();
    filter.setConf(conf);
    Outlink[] links = filter.getJSLinks(js, "anchor", base);
    assertSame(1, links.length);
    assertEquals(expected + "!='" + links[0].getToUrl() + "'", expected,
        links[0].getToUrl());
  }

}
