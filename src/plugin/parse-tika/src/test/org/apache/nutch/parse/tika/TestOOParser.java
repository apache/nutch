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

import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.apache.nutch.parse.ParseException;
import org.apache.nutch.protocol.ProtocolException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Unit tests for OOParser.
 */
public class TestOOParser extends TikaParserTest {

  // Make sure sample files are copied to "test.data" as specified in
  // ./src/plugin/parse-tika/build.xml during plugin compilation.
  private String[] sampleFiles = { "ootest.odt", "ootest.sxw" };

  private String expectedText;

  private String sampleText = "ootest.txt";

  @Test
  public void testIt() throws ProtocolException, ParseException {

    System.out.println("Expected : " + expectedText);

    for (int i = 0; i < sampleFiles.length; i++) {
 
      if (sampleFiles[i].startsWith("ootest") == false)
        continue;

      String text = getTextContent(sampleFiles[i]).replaceAll("[ \t\r\n]+", " ")
          .trim();

      // simply test for the presence of a text - the ordering of the elements
      // may differ from what was expected
      // in the previous tests
      assertFalse(text.isEmpty());

      System.out.println("Found " + sampleFiles[i] + ": " + text);
    }
  }

  public TestOOParser() {
    try {
      // read the test string
      FileInputStream fis = new FileInputStream(sampleDir + fileSeparator
          + sampleText);
      StringBuffer sb = new StringBuffer();
      int len = 0;
      InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
      char[] buf = new char[1024];
      while ((len = isr.read(buf)) > 0) {
        sb.append(buf, 0, len);
      }
      isr.close();
      expectedText = sb.toString();
      // normalize space
      expectedText = expectedText.replaceAll("[ \t\r\n]+", " ");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
