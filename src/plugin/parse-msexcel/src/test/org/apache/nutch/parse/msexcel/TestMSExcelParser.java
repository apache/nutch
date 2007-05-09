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
/*
 *  TestMSExcelParser.java 
 *  Based on the Unit Tests for MSWordParser by John Xing
 */
package org.apache.nutch.parse.msexcel;

// Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.util.NutchConfiguration;

// JUnit imports
import junit.framework.TestCase;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;


/** 
 * Based on Unit tests for MSWordParser by John Xing
 *
 * @author Rohit Kulkarni & Ashish Vaidya
 */
public class TestMSExcelParser extends TestCase {

  private String fileSeparator = System.getProperty("file.separator");
  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data",".");
  
  // Make sure sample files are copied to "test.data"
  
  private String[] sampleFiles = {"test.xls"};

  private String expectedText = "BitStream test.xls 321654.0 Apache " +
                                "incubator 1234.0 Doug Cutting 89078.0 " +
                                "CS 599 Search Engines Spring 2005.0 SBC " +
                                "1234.0 764893.0 Java NUTCH!! ";

  public TestMSExcelParser(String name) { 
    super(name);
  }

  public void testIt() throws ProtocolException, ParseException {

    String urlString;
    Protocol protocol;
    Content content;
    Parse parse;

    Configuration conf = NutchConfiguration.create();
    ParseUtil parser = new ParseUtil(conf);
    ProtocolFactory factory = new ProtocolFactory(conf);
    for (int i = 0; i < sampleFiles.length; i++) {
      urlString = "file:" + sampleDir + fileSeparator + sampleFiles[i];

      protocol = factory.getProtocol(urlString);
      content = protocol.getProtocolOutput(new Text(urlString),
                                           new CrawlDatum()).getContent();
      parse = parser.parseByExtensionId("parse-msexcel", content).get(content.getUrl());

      assertTrue(parse.getText().equals(expectedText));
    }
  }

}
