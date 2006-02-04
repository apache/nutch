/**
 * Copyright 2005 The Apache Software Foundation
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

package org.apache.nutch.parse.msword;

import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolException;

import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import org.apache.hadoop.io.UTF8;
import org.apache.nutch.crawl.CrawlDatum;

import junit.framework.TestCase;

/** 
 * Unit tests for MSWordParser.
 *
 * @author John Xing
 */
public class TestMSWordParser extends TestCase {

  private String fileSeparator = System.getProperty("file.separator");
  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data",".");
  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/parse-msword/build.xml during plugin compilation.
  // Check ./src/plugin/parse-msword/sample/README.txt for what they are.
  private String[] sampleFiles = {"word95.doc","word97.doc"};

  private String expectedText = "This is a sample doc file prepared for nutch.";

  public TestMSWordParser(String name) { 
    super(name); 
  }

  protected void setUp() {}

  protected void tearDown() {}

  public void testIt() throws ProtocolException, ParseException {
    String urlString;
    Protocol protocol;
    Content content;
    Parse parse;

    Configuration conf = NutchConfiguration.create();
    for (int i=0; i<sampleFiles.length; i++) {
      urlString = "file:" + sampleDir + fileSeparator + sampleFiles[i];

      protocol = new ProtocolFactory(conf).getProtocol(urlString);
      content = protocol.getProtocolOutput(new UTF8(urlString), new CrawlDatum()).getContent();
      parse = new ParseUtil(conf).parseByParserId("parse-msword",content);

      assertTrue(parse.getText().startsWith(expectedText));
    }
  }

}
