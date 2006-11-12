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

package org.apache.nutch.parse.mp3;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

/**
 * Unit tests for TestMP3Parser.  (Adapted from John Xing msword unit tests).
 *
 * @author Andy Hedges
 */
public class TestMP3Parser extends TestCase {

  private String fileSeparator = System.getProperty("file.separator");
  // This system property is defined in ./src/plugin/build-plugin.xml
  private String sampleDir = System.getProperty("test.data", ".");
  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/parse-mp3/build.xml during plugin compilation.
  // Check ./src/plugin/parse-mp3/sample/README.txt for what they are.
  private String id3v1 = "postgresql-id3v1.mp3";
  private String id3v2 = "postgresql-id3v2.mp3";
  private String none = "postgresql-none.mp3";

  public TestMP3Parser(String name) {
    super(name);
  }

  protected void setUp() {
  }

  protected void tearDown() {
  }

  public void testId3v2() throws ProtocolException, ParseException {

    String urlString;
    Protocol protocol;
    Content content;
    Parse parse;

    Configuration conf = NutchConfiguration.create();
    urlString = "file:" + sampleDir + fileSeparator + id3v2;
    protocol = new ProtocolFactory(conf).getProtocol(urlString);
    content = protocol.getProtocolOutput(new Text(urlString), new CrawlDatum())
                      .getContent();
    parse = new ParseUtil(conf).parseByExtensionId("parse-mp3", content);
    Metadata metadata = parse.getData().getParseMeta();
    assertEquals("postgresql comment id3v2", metadata.get("COMM-Text"));
    assertEquals("postgresql composer id3v2", metadata.get("TCOM-Text"));
    assertEquals("02", metadata.get("TRCK-Text"));
    assertEquals("http://localhost/", metadata.get("WCOP-URL Link"));
    assertEquals("postgresql artist id3v2", metadata.get("TPE1-Text"));
    assertEquals("(28)", metadata.get("TCON-Text"));
    assertEquals("2004", metadata.get("TYER-Text"));
    assertEquals("postgresql title id3v2", metadata.get("TIT2-Text"));
    assertEquals("postgresql album id3v2", metadata.get("TALB-Text"));
    assertEquals("postgresql encoded by id3v2", metadata.get("TENC-Text"));

    assertEquals("postgresql title id3v2 - "
        + "postgresql album id3v2 - "
        + "postgresql artist id3v2", parse.getData().getTitle());
    assertEquals("http://localhost/", parse.getData().getOutlinks()[0].getToUrl());

  }

  public void testId3v1() throws ProtocolException, ParseException {

    String urlString;
    Protocol protocol;
    Content content;
    Parse parse;

    Configuration conf = NutchConfiguration.create();
    urlString = "file:" + sampleDir + fileSeparator + id3v1;
    protocol = new ProtocolFactory(conf).getProtocol(urlString);
    content = protocol.getProtocolOutput(new Text(urlString), new CrawlDatum())
                      .getContent();
    parse = new ParseUtil(conf).parseByExtensionId("parse-mp3", content);

    Metadata metadata = parse.getData().getParseMeta();
    assertEquals("postgresql comment id3v1", metadata.get("COMM-Text"));
    assertEquals("postgresql artist id3v1", metadata.get("TPE1-Text"));
    assertEquals("(28)", metadata.get("TCON-Text"));
    assertEquals("2004", metadata.get("TYER-Text"));
    assertEquals("postgresql title id3v1", metadata.get("TIT2-Text"));
    assertEquals("postgresql album id3v1", metadata.get("TALB-Text"));

    assertEquals("postgresql title id3v1 - "
        + "postgresql album id3v1 - "
        + "postgresql artist id3v1", parse.getData().getTitle());

  }

  public void testNone() throws ProtocolException, ParseException {
    String urlString;
    Protocol protocol;
    Content content;
    Parse parse;

    Configuration conf = NutchConfiguration.create();
    urlString = "file:" + sampleDir + fileSeparator + none;
    protocol = new ProtocolFactory(conf).getProtocol(urlString);
    content = protocol.getProtocolOutput(new Text(urlString), new CrawlDatum())
                      .getContent();
    parse = new ParseUtil(conf).parseByExtensionId("parse-mp3", content);
//    Metadata metadata = parse.getData().getParseMeta();
    if (parse.getData().getStatus().isSuccess()) {
      fail("Expected ParseException");
    }
  }

}
