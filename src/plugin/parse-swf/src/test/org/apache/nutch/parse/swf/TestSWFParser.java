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

package org.apache.nutch.parse.swf;

import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.hadoop.io.Text;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolException;

import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

/** 
 * Unit tests for SWFParser.
 *
 * @author Andrzej Bialecki
 */
public class TestSWFParser extends TestCase {

  private String fileSeparator = System.getProperty("file.separator");
  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data",".");
  
  private String[] sampleFiles = new String[]{"test1.swf", "test2.swf", "test3.swf"};
  private String[] sampleTexts = new String[]{"test1.txt", "test2.txt", "test3.txt"};
  private String[] texts = new String[sampleTexts.length];

  public TestSWFParser(String name) { 
    super(name);
    for (int i = 0; i < sampleFiles.length; i++) {
    try {
      // read the test string
      FileInputStream fis = new FileInputStream(sampleDir + fileSeparator + sampleTexts[i]);
      StringBuffer sb = new StringBuffer();
      int len = 0;
      InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
      char[] buf = new char[1024];
      while ((len = isr.read(buf)) > 0) {
        sb.append(buf, 0, len);
      }
      isr.close();
      sampleTexts[i] = sb.toString().replaceAll("[ \t\r\n]+", " ").trim();
    } catch (Exception e) {
      e.printStackTrace();
    }
    }
  }

  protected void setUp() {}

  protected void tearDown() {}

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

      parse = new ParseUtil(conf).parse(content).get(content.getUrl());

      String text = parse.getText().replaceAll("[ \t\r\n]+", " ").trim();
      assertTrue(sampleTexts[i].equals(text));
    }
  }

}
