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

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolNotFound;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatusCodes;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * @author mattmann
 * @version $Revision$
 * 
 * <p>
 * Unit tests for the {@link File}Protocol.
 * </p>.
 */
public class TestProtocolFile {

  private String fileSeparator = System.getProperty("file.separator");
  private String sampleDir = System.getProperty("test.data", ".");

  private static final String[] testTextFiles = new String[] {
      "testprotocolfile.txt", "testprotocolfile_(encoded).txt", "testprotocolfile_%28encoded%29.txt" };

  private static final String expectedMimeType = "text/plain";
  
  private Configuration conf;
  
  @Before
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
   * Tests the setting of the <code>Response.CONTENT_TYPE</code> metadata
   * field.
   * @throws ProtocolNotFound 
   * 
   * @since NUTCH-384
   * 
   */
  public void setContentType(String testTextFile) throws ProtocolNotFound {
    String urlString = "file:" + sampleDir + fileSeparator + testTextFile;
    assertNotNull(urlString);
    WebPage datum = new WebPage();
    Protocol protocol = new ProtocolFactory(conf).getProtocol(urlString);
    ProtocolOutput output = protocol.getProtocolOutput(urlString,datum);
    assertNotNull(output);

    assertEquals("Status code: [" + output.getStatus().getCode()
        + "], not equal to: [" + ProtocolStatusCodes.SUCCESS + "]: args: ["
        + output.getStatus().getArgs() + "]", ProtocolStatusCodes.SUCCESS, output
        .getStatus().getCode());
    assertNotNull(output.getContent());
    assertNotNull(output.getContent().getContentType());
    assertEquals(expectedMimeType, output.getContent().getContentType());
    assertNotNull(output.getContent().getMetadata());
    assertEquals(expectedMimeType, output.getContent().getMetadata().get(
        Response.CONTENT_TYPE));

  }

}
