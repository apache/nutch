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

package org.apache.nutch.parse.rtf;

// JUnit imports
import junit.framework.TestCase;

// Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.DublinCore;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.util.NutchConfiguration;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;


/**
 * Unit tests for TestRTFParser.  (Adapted from John Xing msword unit tests).
 *
 * @author Andy Hedges
 */
public class TestRTFParser extends TestCase {

  private String fileSeparator = System.getProperty("file.separator");
  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data", ".");
  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/parse-rtf/build.xml during plugin compilation.
  // Check ./src/plugin/parse-rtf/sample/README.txt for what they are.
  private String rtfFile = "test.rtf";

  public TestRTFParser(String name) {
    super(name);
  }

  protected void setUp() {
  }

  protected void tearDown() {
  }

  public void testIt() throws ProtocolException, ParseException {

    String urlString;
    Protocol protocol;
    Content content;
    Parse parse;

    Configuration conf = NutchConfiguration.create();
    urlString = "file:" + sampleDir + fileSeparator + rtfFile;
    protocol = new ProtocolFactory(conf).getProtocol(urlString);
    content = protocol.getProtocolOutput(new Text(urlString), new CrawlDatum())
                      .getContent();
    parse = new ParseUtil(conf).parseByExtensionId("parse-rtf", content);
    String text = parse.getText();
    assertEquals("The quick brown fox jumps over the lazy dog", text.trim());

    String title = parse.getData().getTitle();
    Metadata meta = parse.getData().getParseMeta();
    assertEquals("test rft document", title);
    assertEquals("tests", meta.get(DublinCore.SUBJECT));



  }


}
