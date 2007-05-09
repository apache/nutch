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

package org.apache.nutch.parse.rss;

import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolException;

import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.Outlink;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;

import junit.framework.TestCase;

/**
 * Unit tests for the RSS Parser based on John Xing's TestPdfParser class.
 * 
 * @author mattmann
 * @version 1.0
 */
public class TestRSSParser extends TestCase {

    private String fileSeparator = System.getProperty("file.separator");

    // This system property is defined in ./src/plugin/build-plugin.xml
    private String sampleDir = System.getProperty("test.data", ".");

    // Make sure sample files are copied to "test.data" as specified in
    // ./src/plugin/parse-rss/build.xml during plugin compilation.

    private String[] sampleFiles = { "rsstest.rss" };

    /**
     * <p>
     * Default constructor
     * </p>
     * 
     * @param name
     *            The name of the RSSParserTest
     */
    public TestRSSParser(String name) {
        super(name);
    }

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
    public void testIt() throws ProtocolException, ParseException {
        String urlString;
        Protocol protocol;
        Content content;
        Parse parse;

        Configuration conf = NutchConfiguration.create();
        for (int i = 0; i < sampleFiles.length; i++) {
            urlString = "file:" + sampleDir + fileSeparator + sampleFiles[i];

            protocol = new ProtocolFactory(conf).getProtocol(urlString);
            content = protocol.getProtocolOutput(new Text(urlString), new CrawlDatum()).getContent();
            parse = new ParseUtil(conf).parseByExtensionId("parse-rss", content).get(content.getUrl());

            //check that there are 3 outlinks:
            //http://test.channel.com
            //http://www-scf.usc.edu/~mattmann/
            //http://www.nutch.org

            ParseData theParseData = parse.getData();

            Outlink[] theOutlinks = theParseData.getOutlinks();

            assertTrue("There aren't 3 outlinks read!", theOutlinks.length == 3);

            //now check to make sure that those are the two outlinks
            boolean hasLink1 = false, hasLink2 = false, hasLink3 = false;

            for (int j = 0; j < theOutlinks.length; j++) {
                //System.out.println("reading "+theOutlinks[j].getToUrl());
                if (theOutlinks[j].getToUrl().equals(
                        "http://www-scf.usc.edu/~mattmann/")) {
                    hasLink1 = true;
                }

                if (theOutlinks[j].getToUrl().equals("http://www.nutch.org/")) {
                    hasLink2 = true;
                }

                if (theOutlinks[j].getToUrl()
                        .equals("http://test.channel.com/")) {
                    hasLink3 = true;
                }
            }

            if (!hasLink1 || !hasLink2 || !hasLink3) {
                fail("Outlinks read from sample rss file are not correct!");
            }
        }
    }

}
