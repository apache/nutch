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

import org.apache.nutch.parse.ParseException;
import org.apache.nutch.protocol.ProtocolException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for MSWordParser.
 */
public class TestEmbeddedDocuments extends TikaParserTest {

  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/parse-tika/build.xml during plugin compilation.
  private String[] sampleFiles = { "test_recursive_embedded.docx" };

  private String expectedText = "When in the Course of human events";

  @Before
  public void setUp() {
    super.setUp();
    conf.setBoolean("tika.parse.embedded", true);
  }

  @Test
  public void testIt() throws ProtocolException, ParseException {
    for (int i = 0; i < sampleFiles.length; i++) {
      String found = getTextContent(sampleFiles[i]);
      Assert.assertTrue("text found : '" + found + "'",
          found.contains(expectedText));
    }
  }

}
