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

package org.apache.nutch.parse.replace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class TestParseReplace {

  private String fileSeparator = System.getProperty("file.separator");
  private String sampleDir = System.getProperty("test.data", ".");
  private String sampleFile = "testParseReplace.html";
  private String description = "This is a test of description";
  private String keywords = "This is a test of keywords";

  public Metadata parseMeta(String fileName, Configuration conf) {
    Metadata metadata = null;
    try {
      String urlString = "file:" + sampleDir + fileSeparator + fileName;
      Protocol protocol = new ProtocolFactory(conf).getProtocol(urlString);
      Content content = protocol.getProtocolOutput(new Text(urlString),
          new CrawlDatum()).getContent();
      Parse parse = new ParseUtil(conf).parse(content).get(content.getUrl());
      metadata = parse.getData().getParseMeta();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.toString());
    }
    return metadata;
  }

  @Test
  /** test defaults: keywords and description */
  public void testIt() {
    Configuration conf = NutchConfiguration.create();

    // check that we get the same values
    Metadata parseMeta = parseMeta(sampleFile, conf);

    Assert.assertEquals(description, parseMeta.get("metatag.description"));
    Assert.assertEquals(keywords, parseMeta.get("metatag.keywords"));
  }
}
