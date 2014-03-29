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

import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/** 
 * Unit tests for MSWordParser.
 *
 * @author John Xing
 */
public class TestMSWordParser {

  private String fileSeparator = System.getProperty("file.separator");
  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data",".");
  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/parse-msword/build.xml during plugin compilation.
  // Check ./src/plugin/parse-msword/sample/README.txt for what they are.
  private String[] sampleFiles = {"word97.doc"};

  private String expectedText = "This is a sample doc file prepared for nutch.";

  private Configuration conf;

  @Before
  public void setUp() {
    conf = NutchConfiguration.create();
    conf.set("file.content.limit", "-1");
  }

  public String getTextContent(String fileName) throws ProtocolException, ParseException {
    String urlString = "file:" + sampleDir + fileSeparator + fileName;
    Protocol protocol = new ProtocolFactory(conf).getProtocol(urlString);
    Content content = protocol.getProtocolOutput(new Text(urlString), new CrawlDatum()).getContent();
    Parse parse = new ParseUtil(conf).parseByExtensionId("parse-tika", content).get(content.getUrl());
    return parse.getText();
  }

  @Test
  public void testIt() throws ProtocolException, ParseException {
    for (int i=0; i<sampleFiles.length; i++) {
      String found = getTextContent(sampleFiles[i]);
      Assert.assertTrue("text found : '"+found+"'",found.startsWith(expectedText));
    }
  }

  @Test
  public void testOpeningDocs() throws ProtocolException, ParseException {
    String[] filenames = new File(sampleDir).list();
    for (int i = 0; i < filenames.length; i++) {
      if (filenames[i].endsWith(".doc")==false) continue;
      Assert.assertTrue("cann't read content of " + filenames[i], getTextContent(filenames[i]).length() > 0);
    }      
  }
}
