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

package org.apache.nutch.tika;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.tika.TikaParser;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.NutchConfiguration;

/**
 * 
 * @author mattmann / jnioche
 * 
 *         Test Suite for the RSS feeds with the {@link TikaParser}.
 * 
 */
public class TestFeedParser {

  private String fileSeparator = System.getProperty("file.separator");

  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data", ".");

  private String[] sampleFiles = { "rsstest.rss" };

  public static final Logger LOG = LoggerFactory.getLogger(TestFeedParser.class
      .getName());

  /**
   * <p>
   * The test method: tests out the following 2 asserts:
   * </p>
   * 
   * <ul>
   * <li>There are 3 outlinks read from the sample rss file</li>
   * <li>The 3 outlinks read are in fact the correct outlinks from the sample
   * file</li>
   * </ul>
   */
  @Test
  public void testIt() throws ProtocolException, ParseException {
    String urlString;
    Protocol protocol;
    Content content;
    Parse parse;

    Configuration conf = NutchConfiguration.create();
    for (int i = 0; i < sampleFiles.length; i++) {
      urlString = "file:" + sampleDir + fileSeparator + sampleFiles[i];

      protocol = new ProtocolFactory(conf).getProtocol(urlString);
      content = protocol.getProtocolOutput(new Text(urlString),
          new CrawlDatum()).getContent();
      parse = new ParseUtil(conf).parseByExtensionId("parse-tika",
          content).get(content.getUrl());

      // check that there are 2 outlinks:
      // unlike the original parse-rss
      // tika ignores the URL and description of the channel

      // http://test.channel.com
      // http://www-scf.usc.edu/~mattmann/
      // http://www.nutch.org

      ParseData theParseData = parse.getData();

      Outlink[] theOutlinks = theParseData.getOutlinks();

      Assert.assertTrue("There aren't 2 outlinks read!",
          theOutlinks.length == 2);

      // now check to make sure that those are the two outlinks
      boolean hasLink1 = false, hasLink2 = false;

      for (int j = 0; j < theOutlinks.length; j++) {
        if (theOutlinks[j].getToUrl().equals(
            "http://www-scf.usc.edu/~mattmann/")) {
          hasLink1 = true;
        }

        if (theOutlinks[j].getToUrl().equals("http://www.nutch.org/")) {
          hasLink2 = true;
        }
      }

      if (!hasLink1 || !hasLink2) {
        Assert.fail("Outlinks read from sample rss file are not correct!");
      }
    }
  }

}
