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

package org.apache.nutch.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;

import com.google.common.io.Files;

import junit.framework.TestCase;

public class TestMimeUtil extends TestCase {

  public static String urlPrefix = "http://localhost/";

  private static Charset defaultCharset = Charset.forName("UTF-8");

  private File sampleDir = new File(System.getProperty("test.build.data", "."),
      "test-mime-util");

  /** test data, every element on "test page":
   * <ol>
   * <li>MIME type</li>
   * <li>file name (last URL path element)</li>
   * <li>Content-Type (HTTP header)</li>
   * <li>content: if empty, do not test MIME magic</li>
   * </ol>
   */
  public static String[][] textBasedFormats = {
      {
          "text/html",
          "test.html",
          "text/html; charset=utf-8",
          "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" "
              + "\"http://www.w3.org/TR/html4/loose.dtd\">\n"
              + "<html>\n<head>\n"
              + "<meta http-equiv=Content-Type content=\"text/html; charset=utf-8\" />\n"
              + "</head>\n<body>Hello, World!</body></html>" },
      {
          "text/html",
          "test.html",
          "", // no Content-Type in HTTP header => test URL pattern
          "<!DOCTYPE html>\n<html>\n<head>\n"
              + "</head>\n<body>Hello, World!</body></html>" },
      {
          "application/xhtml+xml",
          "test.html",
          "application/xhtml+xml; charset=utf-8",
          "<?xml version=\"1.0\"?>\n<html xmlns=\"http://www.w3.org/1999/xhtml\">"
              + "<html>\n<head>\n"
              + "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
              + "</head>\n<body>Hello, World!</body></html>" }
    };

  public static String[][] binaryFiles = {
    {
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "test.xlsx",
      "" }
    };

  private String getMimeType(String url, File file, String contentType,
      boolean useMagic) throws IOException {
    return getMimeType(url, Files.toByteArray(file), contentType, useMagic);
  }

  private String getMimeType(String url, byte[] bytes, String contentType,
      boolean useMagic) {
    Configuration conf = NutchConfiguration.create();
    conf.setBoolean("mime.type.magic", useMagic);
    MimeUtil mimeUtil = new MimeUtil(conf);
    return mimeUtil.autoResolveContentType(contentType, url, bytes);
  }

  /** use HTTP Content-Type, URL pattern, and MIME magic */
  public void testWithMimeMagic() {
    for (String[] testPage : textBasedFormats) {
      String mimeType = getMimeType(urlPrefix,
          testPage[3].getBytes(defaultCharset), testPage[2], true);
      assertEquals("", testPage[0], mimeType);
    }
  }

  /** use only HTTP Content-Type (if given) and URL pattern */
  public void testWithoutMimeMagic() {
    for (String[] testPage : textBasedFormats) {
      String mimeType = getMimeType(urlPrefix + testPage[1],
          testPage[3].getBytes(defaultCharset), testPage[2], false);
      assertEquals("", testPage[0], mimeType);
    }
  }

  /** use only MIME magic (detection from content bytes) */
  public void testOnlyMimeMagic() {
    for (String[] testPage : textBasedFormats) {
      String mimeType = getMimeType(urlPrefix,
          testPage[3].getBytes(defaultCharset), "", true);
      assertEquals("", testPage[0], mimeType);
    }
  }

  /** test binary file formats (real files) */
  public void testBinaryFiles() throws IOException {
    for (String[] testPage : binaryFiles) {
      File dataFile = new File(sampleDir, testPage[1]);
      String mimeType = getMimeType(urlPrefix + testPage[1],
          dataFile, testPage[2], false);
      assertEquals("", testPage[0], mimeType);
    }
  }

}
