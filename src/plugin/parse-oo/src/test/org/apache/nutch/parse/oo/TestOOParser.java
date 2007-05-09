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

package org.apache.nutch.parse.oo;

import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.protocol.*;

import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

/** 
 * Unit tests for OOParser.
 *
 * @author Andrzej Bialecki
 */
public class TestOOParser extends TestCase {

  private String fileSeparator = System.getProperty("file.separator");
  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data",".");
  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/parse-oo/build.xml during plugin compilation.
  private String[] sampleFiles = {"ootest.odt", "ootest.sxw"};

  private String sampleText = "ootest.txt";
  
  private String expectedText;

  public TestOOParser(String name) { 
    super(name);
    try {
      // read the test string
      FileInputStream fis = new FileInputStream(sampleDir + fileSeparator + sampleText);
      StringBuffer sb = new StringBuffer();
      int len = 0;
      InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
      char[] buf = new char[1024];
      while ((len = isr.read(buf)) > 0) {
        sb.append(buf, 0, len);
      }
      isr.close();
      expectedText = sb.toString();
      // normalize space
      expectedText = expectedText.replaceAll("[ \t\r\n]+", " ");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected void setUp() {}

  protected void tearDown() {}

  public void testIt() throws ProtocolException, ParseException {
    String urlString;
    Content content;
    Parse parse;
    Configuration conf = NutchConfiguration.create();
    Protocol protocol;
    ProtocolFactory factory = new ProtocolFactory(conf);
    OOParser parser = new OOParser();
    parser.setConf(conf);

    for (int i=0; i<sampleFiles.length; i++) {
      urlString = "file:" + sampleDir + fileSeparator + sampleFiles[i];

      protocol = factory.getProtocol(urlString);
      content = protocol.getProtocolOutput(new Text(urlString), new CrawlDatum()).getContent();

      parse = parser.getParse(content).get(content.getUrl());

      String text = parse.getText().replaceAll("[ \t\r\n]+", " ");
      assertTrue(expectedText.equals(text));
    }
  }

}
