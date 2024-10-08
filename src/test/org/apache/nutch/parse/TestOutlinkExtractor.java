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

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Test;

/**
 * TestCase to check regExp extraction of URLs.
 * 
 * @author Stephan Strittmatter - http://www.sybit.de
 * 
 * @version 1.0
 */
public class TestOutlinkExtractor {

  private static Configuration conf = NutchConfiguration.create();

  @Test
  public void testGetNoOutlinks() {
    Outlink[] outlinks = null;

    outlinks = OutlinkExtractor.getOutlinks(null, conf);
    Assert.assertNotNull(outlinks);
    Assert.assertEquals(0, outlinks.length);

    outlinks = OutlinkExtractor.getOutlinks("", conf);
    Assert.assertNotNull(outlinks);
    Assert.assertEquals(0, outlinks.length);
  }

  @Test
  public void testGetOutlinksHttp() {
    Outlink[] outlinks = OutlinkExtractor
        .getOutlinks(
            "Test with http://www.nutch.org/index.html is it found? "
                + "What about www.google.com at http://www.google.de "
                + "A longer URL could be http://www.sybit.com/solutions/portals.html",
            conf);

    Assert.assertTrue("Url not found!", outlinks.length == 3);
    Assert.assertEquals("Wrong URL", "http://www.nutch.org/index.html",
        outlinks[0].getToUrl());
    Assert.assertEquals("Wrong URL", "http://www.google.de",
        outlinks[1].getToUrl());
    Assert.assertEquals("Wrong URL",
        "http://www.sybit.com/solutions/portals.html", outlinks[2].getToUrl());
  }

  @Test
  public void testGetOutlinksHttp2() {
    Outlink[] outlinks = OutlinkExtractor
        .getOutlinks(
            "Test with http://www.nutch.org/index.html is it found? "
                + "What about www.google.com at http://www.google.de "
                + "A longer URL could be http://www.sybit.com/solutions/portals.html",
            "http://www.sybit.de", conf);

    Assert.assertTrue("Url not found!", outlinks.length == 3);
    Assert.assertEquals("Wrong URL", "http://www.nutch.org/index.html",
        outlinks[0].getToUrl());
    Assert.assertEquals("Wrong URL", "http://www.google.de",
        outlinks[1].getToUrl());
    Assert.assertEquals("Wrong URL",
        "http://www.sybit.com/solutions/portals.html", outlinks[2].getToUrl());
  }

  @Test
  public void testGetOutlinksFtp() {
    Outlink[] outlinks = OutlinkExtractor.getOutlinks(
        "Test with ftp://www.nutch.org is it found? "
            + "What about www.google.com at ftp://www.google.de", conf);

    Assert.assertTrue("Url not found!", outlinks.length > 1);
    Assert.assertEquals("Wrong URL", "ftp://www.nutch.org",
        outlinks[0].getToUrl());
    Assert.assertEquals("Wrong URL", "ftp://www.google.de",
        outlinks[1].getToUrl());
  }
}
