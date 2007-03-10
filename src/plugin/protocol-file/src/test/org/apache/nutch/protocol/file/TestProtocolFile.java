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

// Hadoop imports
import org.apache.hadoop.io.Text;

// Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.util.NutchConfiguration;

// Junit imports
import junit.framework.TestCase;

/**
 * @author mattmann
 * @version $Revision$
 * 
 * <p>
 * Unit tests for the {@link File}Protocol.
 * </p>.
 */
public class TestProtocolFile extends TestCase {

  private static final org.apache.nutch.protocol.file.File fileProtocol = 
    new org.apache.nutch.protocol.file.File();

  private static final String testTextFile = "testprotocolfile.txt";

  private static final CrawlDatum datum = new CrawlDatum();

  private static final String expectedMimeType = "text/plain";

  static {
    fileProtocol.setConf(NutchConfiguration.create());
  }

  /**
   * Tests the setting of the <code>Response.CONTENT_TYPE</code> metadata
   * field.
   * 
   * @since NUTCH-384
   * 
   */
  public void testSetContentType() {
    Text fileUrl = new Text(this.getClass().getResource(testTextFile)
        .toString());
    assertNotNull(fileUrl);
    ProtocolOutput output = fileProtocol.getProtocolOutput(fileUrl, datum);
    assertNotNull(output);
    assertEquals("Status code: [" + output.getStatus().getCode()
        + "], not equal to: [" + ProtocolStatus.SUCCESS + "]: args: ["
        + output.getStatus().getArgs() + "]", ProtocolStatus.SUCCESS, output
        .getStatus().getCode());
    assertNotNull(output.getContent());
    assertNotNull(output.getContent().getContentType());
    assertEquals(expectedMimeType, output.getContent().getContentType());
    assertNotNull(output.getContent().getMetadata());
    assertEquals(expectedMimeType, output.getContent().getMetadata().get(
        Response.CONTENT_TYPE));

  }

}
