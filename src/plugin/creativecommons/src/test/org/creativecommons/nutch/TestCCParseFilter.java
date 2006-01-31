/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.creativecommons.nutch;

import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ContentProperties;
import org.apache.nutch.util.NutchConf;

import java.util.Properties;
import java.io.*;
import java.net.URL;

import junit.framework.TestCase;

public class TestCCParseFilter extends TestCase {

  private static final File testDir =
    new File(System.getProperty("test.input"));

  public void testPages() throws Exception {
    pageTest(new File(testDir, "anchor.html"), "http://foo.com/",
             "http://creativecommons.org/licenses/by-nc-sa/1.0", "a", null);
    pageTest(new File(testDir, "rel.html"), "http://foo.com/",
             "http://creativecommons.org/licenses/by-nc/2.0", "rel", null);
    pageTest(new File(testDir, "rdf.html"), "http://foo.com/",
             "http://creativecommons.org/licenses/by-nc/1.0", "rdf", "text");
  }

  public void pageTest(File file, String url,
                       String license, String location, String type)
    throws Exception {

    String contentType = "text/html";
    InputStream in = new FileInputStream(file);
    ByteArrayOutputStream out = new ByteArrayOutputStream((int)file.length());
    byte[] buffer = new byte[1024];
    int i;
    while ((i = in.read(buffer)) != -1) {
      out.write(buffer, 0, i);
    }
    in.close();
    byte[] bytes = out.toByteArray();
    NutchConf nutchConf = new NutchConf();

    Content content =
      new Content(url, url, bytes, contentType, new ContentProperties(), nutchConf);
    Parse parse = new ParseUtil(nutchConf).parseByParserId("parse-html",content);

    ContentProperties metadata = parse.getData().getMetadata();
    assertEquals(license, metadata.get("License-Url"));
    assertEquals(location, metadata.get("License-Location"));
    assertEquals(type, metadata.get("Work-Type"));
  }
}
