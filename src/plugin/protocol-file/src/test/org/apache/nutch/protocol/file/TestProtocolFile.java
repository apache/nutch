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
package org.apache.nutch.protocol.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author mattmann
 * 
 * Unit tests for the {@link File} Protocol.
 */
@Tag("file")
public class TestProtocolFile {

  private String fileSeparator = System.getProperty("file.separator");
  private String sampleDir = System.getProperty("test.data", ".");

  private static final String[] testTextFiles = new String[] {
      "testprotocolfile.txt", "testprotocolfile_(encoded).txt",
      "testprotocolfile_%28encoded%29.txt" };

  private static final CrawlDatum datum = new CrawlDatum();

  private static final String expectedMimeType = "text/plain";

  private Configuration conf;

  @BeforeEach
  public void setUp() {
    conf = NutchConfiguration.create();
  }

  @Test
  public void testSetContentType() throws ProtocolException {
    for (String testTextFile : testTextFiles) {
      setContentType(testTextFile);
    }
  }

  /**
   * Tests the setting of the <code>Response.CONTENT_TYPE</code> metadata field.
   * 
   * @since NUTCH-384
   * 
   */
  public void setContentType(String testTextFile) throws ProtocolException {
    String urlString = "file:" + sampleDir + fileSeparator + testTextFile;
    Assertions.assertNotNull(urlString);
    Protocol protocol = new ProtocolFactory(conf).getProtocol(urlString);
    ProtocolOutput output = protocol.getProtocolOutput(new Text(urlString),
        datum);
    Assertions.assertNotNull(output);
    Assertions.assertEquals(ProtocolStatus.SUCCESS, output.getStatus().getCode(),
        "Status code: [" + output.getStatus().getCode()
            + "], not equal to: [" + ProtocolStatus.SUCCESS + "]: args: ["
            + output.getStatus().getArgs() + "]");
    Assertions.assertNotNull(output.getContent());
    Assertions.assertNotNull(output.getContent().getContentType());
    Assertions.assertEquals(expectedMimeType, output.getContent().getContentType());
    Assertions.assertNotNull(output.getContent().getMetadata());
    Assertions.assertEquals(expectedMimeType, output.getContent().getMetadata()
        .get(Response.CONTENT_TYPE));
  }

}
