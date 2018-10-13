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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JUnit test case for {@link JSParseFilter} which tests
 * <ol>
 * <li>That 2 outlinks are extracted from JavaScript snippets embedded in
 * HTML</li>
 * <li>That X outlinks are extracted from a pure JavaScript file (this is
 * temporarily disabled)</li>
 * </ol>
 */
public class TestJSParseFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private String fileSeparator = System.getProperty("file.separator");

  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data", ".");

  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/parse-js/build.xml during plugin compilation.

  private Configuration conf;

  @Before
  public void setUp() {
    conf = NutchConfiguration.create();
    conf.set("file.content.limit", "-1");
    conf.set("plugin.includes", "protocol-file|parse-(html|js)");
  }

  public Outlink[] getOutlinks(String sampleFile)
      throws ProtocolException, ParseException, IOException {
    String urlString;
    Parse parse;

    urlString = "file:" + sampleDir + fileSeparator + sampleFile;
    LOG.info("Parsing {}", urlString);
    Protocol protocol = new ProtocolFactory(conf).getProtocol(urlString);
    Content content = protocol
        .getProtocolOutput(new Text(urlString), new CrawlDatum()).getContent();
    parse = new ParseUtil(conf).parse(content).get(content.getUrl());
    LOG.info(parse.getData().toString());
    return parse.getData().getOutlinks();
  }

  @Test
  public void testJavaScriptOutlinkExtraction()
      throws ProtocolException, ParseException, IOException {
    String[] filenames = new File(sampleDir).list();
    for (int i = 0; i < filenames.length; i++) {
      Outlink[] outlinks = getOutlinks(filenames[i]);
      if (filenames[i].endsWith("parse_pure_js_test.js")) {
        assertEquals("number of outlinks in .js test file should be X", 2,
            outlinks.length);
        assertEquals("http://search.lucidimagination.com/p:nutch", outlinks[0].getToUrl());
        assertEquals("http://search-lucene.com/nutch", outlinks[1].getToUrl());
      } else {
        assertTrue("number of outlinks in .html file should be at least 2", outlinks.length >= 2);
        Set<String> outlinkSet = new TreeSet<>();
        for (Outlink o : outlinks) {
          outlinkSet.add(o.getToUrl());
        }
        assertTrue("http://search.lucidimagination.com/p:nutch not in outlinks",
            outlinkSet.contains("http://search.lucidimagination.com/p:nutch"));
        assertTrue("http://search-lucene.com/nutch not in outlinks",
            outlinkSet.contains("http://search-lucene.com/nutch"));
      }
    }
  }

}
