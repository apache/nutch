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

import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.tika.metadata.DublinCore;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for TestRTFParser.
 */
public class TestRTFParser extends TikaParserTest {

  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/parse-tika/build.xml during plugin compilation.
  private String rtfFile = "test.rtf";

  @Test
  public void testIt() throws ProtocolException, ParseException {

    String urlString;
    Protocol protocol;
    Content content;
    Parse parse;

    urlString = "file:" + sampleDir + fileSeparator + rtfFile;
    protocol = new ProtocolFactory(conf).getProtocol(urlString);
    content = protocol.getProtocolOutput(new Text(urlString), new CrawlDatum())
        .getContent();
    parse = new ParseUtil(conf).parseByExtensionId("parse-tika", content).get(
        content.getUrl());
    String text = parse.getText();
    Assert.assertTrue(text.contains("The quick brown fox jumps over the lazy dog"));

    String title = parse.getData().getTitle();
    Metadata meta = parse.getData().getParseMeta();

    Assert.assertEquals("test rft document", title);
    Assert.assertEquals("tests", meta.get(DublinCore.SUBJECT.getName()));

  }
}
