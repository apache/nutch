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

package org.apache.nutch.parse.feed;

// JDK imports
import java.util.Iterator;
import java.util.Map;

// APACHE imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolNotFound;
import org.apache.nutch.util.NutchConfiguration;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * 
 * @author mattmann
 * 
 * Test Suite for the {@link FeedParser}.
 * 
 */
public class TestFeedParser {

  private String fileSeparator = System.getProperty("file.separator");

  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data", ".");

  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/feed/build.xml during plugin compilation.

  private String[] sampleFiles = { "rsstest.rss" };

  public static final Logger LOG = LoggerFactory.getLogger(TestFeedParser.class
      .getName());

  /**
   * Default Constructor.
   * 
   * @param name
   *          The name of this {@link TestCase}.
   */
  public TestFeedParser(String name) {
  }

  /**
   * Calls the {@link FeedParser} on a sample RSS file and checks that there are
   * 3 {@link ParseResult} entries including the below 2 links:
   * <ul>
   * <li>http://www-scf.usc.edu/~mattmann/</li>
   * <li>http://www.nutch.org</li>
   * </ul>
   * 
   * 
   * @throws ProtocolNotFound
   *           If the {@link Protocol}Layer cannot be loaded (required to fetch
   *           the {@link Content} for the RSS file).
   * @throws ParseException
   *           If the {@link Parser}Layer cannot be loaded.
   */
  @Test
  public void testParseFetchChannel() throws ProtocolNotFound, ParseException {
    String urlString;
    Protocol protocol;
    Content content;
    ParseResult parseResult;

    Configuration conf = NutchConfiguration.create();
    for (int i = 0; i < sampleFiles.length; i++) {
      urlString = "file:" + sampleDir + fileSeparator + sampleFiles[i];
      urlString = urlString.replace('\\', '/');

      protocol = new ProtocolFactory(conf).getProtocol(urlString);
      content = protocol.getProtocolOutput(new Text(urlString),
          new CrawlDatum()).getContent();

      parseResult = new ParseUtil(conf).parseByExtensionId("feed", content);

      assertEquals(3, parseResult.size());

      boolean hasLink1 = false, hasLink2 = false, hasLink3=false;

      for (Iterator<Map.Entry<Text, Parse>> j = parseResult.iterator(); j
          .hasNext();) {
        Map.Entry<Text, Parse> entry = j.next();
        if (entry.getKey().toString().equals(
            "http://www-scf.usc.edu/~mattmann/")) {
          hasLink1 = true;
        } else if (entry.getKey().toString().equals("http://www.nutch.org/")) {
          hasLink2 = true;
        }
        else if(entry.getKey().toString().equals(urlString)){
          hasLink3 = true;
        }

        assertNotNull(entry.getValue());
        assertNotNull(entry.getValue().getData());
      }

      if (!hasLink1 || !hasLink2 || !hasLink3) {
        fail("Outlinks read from sample rss file are not correct!");
      }
    }

  }

}
