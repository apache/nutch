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

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.MimeUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * JUnit test case for {@link JSParseFilter} which tests
 * <ol>
 * <li>That 2 outlinks are extracted from JavaScript snippets embedded in
 * HTML</li>
 * <li>That 2 outlinks are extracted from a pure JavaScript file.</li>
 * </ol>
 */
public class TestJSParseFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private String fileSeparator = System.getProperty("file.separator");

  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data", ".");

  private Configuration conf;

  @Before
  public void setUp() {
    conf = NutchConfiguration.create();
    conf.set("plugin.includes", "parse-(html|js)");
  }

  public Outlink[] getOutlinks(String sampleFile)
      throws ProtocolException, ParseException, IOException {
    String urlString, fileName;
    Parse parse;

    fileName = sampleDir + fileSeparator + sampleFile;
    urlString = "file:" + fileName;

    urlString = "file:" + sampleDir + fileSeparator + sampleFile;
    File file = new File(fileName);
    byte[] bytes = new byte[(int) file.length()];
    DataInputStream dip = new DataInputStream(new FileInputStream(file));
    dip.readFully(bytes);
    dip.close();

    LOG.info("Parsing {}", urlString);
    WebPage page = WebPage.newBuilder().build();
    page.setBaseUrl(new Utf8(urlString));
    page.setContent(ByteBuffer.wrap(bytes));
    MimeUtil mutil = new MimeUtil(conf);
    String mime = mutil.getMimeType(file);
    page.setContentType(new Utf8(mime));

    parse = new ParseUtil(conf).parse(urlString, page);
    LOG.info("Parsed {} with {} outlinks: {}", urlString,
        parse.getOutlinks().length, Arrays.toString(parse.getOutlinks()));
    return parse.getOutlinks();
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
