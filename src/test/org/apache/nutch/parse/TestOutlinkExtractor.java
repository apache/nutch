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
package org.apache.nutch.parse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.conf.Configuration;

import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.OutlinkExtractor;
import org.apache.nutch.util.NutchConfiguration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * TestCase to check regExp extraction of URLs.
 * 
 * @author Stephan Strittmatter - http://www.sybit.de
 * 
 * @version 1.0
 */
@Tag("org.apache.nutch.parse")
@Tag("core")
public class TestOutlinkExtractor {

  private static Configuration conf = NutchConfiguration.create();

  @Test
  public void testGetNoOutlinks() {
    Outlink[] outlinks = null;

    outlinks = OutlinkExtractor.getOutlinks(null, conf);
    assertNotNull(outlinks);
    assertEquals(0, outlinks.length);

    outlinks = OutlinkExtractor.getOutlinks("", conf);
    assertNotNull(outlinks);
    assertEquals(0, outlinks.length);
  }

  @Test
  public void testGetOutlinksHttp() {
    Outlink[] outlinks = OutlinkExtractor
        .getOutlinks(
            "Test with http://www.nutch.org/index.html is it found? "
                + "What about www.google.com at http://www.google.de "
                + "A longer URL could be http://www.sybit.com/solutions/portals.html",
            conf);
    assertTrue(outlinks.length == 3, "Url not found!");
    assertEquals("http://www.nutch.org/index.html",
        outlinks[0].getToUrl(), "Wrong URL");
    assertEquals("http://www.google.de", outlinks[1].getToUrl(), "Wrong URL");
    assertEquals("http://www.sybit.com/solutions/portals.html",
        outlinks[2].getToUrl(), "Wrong URL");
  }

  @Test
  public void testGetOutlinksHttp2() {
    Outlink[] outlinks = OutlinkExtractor
        .getOutlinks(
            "Test with http://www.nutch.org/index.html is it found? "
                + "What about www.google.com at http://www.google.de "
                + "A longer URL could be http://www.sybit.com/solutions/portals.html",
            "http://www.sybit.de", conf);
    assertTrue(outlinks.length == 3, "Url not found!");
    assertEquals("http://www.nutch.org/index.html",
        outlinks[0].getToUrl(), "Wrong URL");
    assertEquals("http://www.google.de", outlinks[1].getToUrl(), "Wrong URL");
    assertEquals("http://www.sybit.com/solutions/portals.html",
        outlinks[2].getToUrl(), "Wrong URL");
  }

  @Test
  public void testGetOutlinksFtp() {
    Outlink[] outlinks = OutlinkExtractor.getOutlinks(
        "Test with ftp://www.nutch.org is it found? "
            + "What about www.google.com at ftp://www.google.de", conf);
    assertTrue(outlinks.length > 1, "Url not found!");
    assertEquals("ftp://www.nutch.org", outlinks[0].getToUrl(), "Wrong URL");
    assertEquals("ftp://www.google.de", outlinks[1].getToUrl(), "Wrong URL");
  }
}
