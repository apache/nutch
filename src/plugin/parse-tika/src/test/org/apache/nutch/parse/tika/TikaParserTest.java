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
package org.apache.nutch.parse.tika;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Before;

/**
 * Base class to extend Tika parser tests from.
 */
public class TikaParserTest {

  protected String fileSeparator = System.getProperty("file.separator");

  /**
   * Folder with test data, defined in src/plugin/build-plugin.xml. Make sure
   * that all sample files are copied to "test.data", they must be listed in
   * src/plugin/parse-tika/build.xml
   */
  protected String sampleDir = System.getProperty("test.data", ".");

  protected Configuration conf;

  @Before
  public void setUp() {
    conf = NutchConfiguration.create();
    conf.set("file.content.limit", "-1");
  }

  public String getTextContent(String fileName)
      throws ProtocolException, ParseException {
    String urlString = "file:" + sampleDir + fileSeparator + fileName;
    Protocol protocol = new ProtocolFactory(conf).getProtocol(urlString);
    Content content = protocol
        .getProtocolOutput(new Text(urlString), new CrawlDatum()).getContent();
    Parse parse = new ParseUtil(conf).parseByExtensionId("parse-tika", content)
        .get(content.getUrl());
    return parse.getText();
  }

}
