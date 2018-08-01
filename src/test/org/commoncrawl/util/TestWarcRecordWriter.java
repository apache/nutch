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
package org.commoncrawl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

public class TestWarcRecordWriter {

  public final static String statusLine1 = "HTTP/1.1 200 OK";
  public final static String testHeaders1[] = { //
      "Content-Type", "text/html", //
      "Accept-Ranges", "bytes", //
      "Content-Encoding", "gzip", //
      "Vary", "Accept-Encoding", "Server",
      "Apache/2.0.63 (Unix) PHP/4.4.7 mod_ssl/2.0.63 OpenSSL/0.9.7e mod_fastcgi/2.4.2 DAV/2 SVN/1.4.2",
      "Last-Modified", "Thu, 15 Jan 2009 00:02:29 GMT", "ETag",
      "\"1262d9e-3ffa-2c19af40\"", //
      "Date", "Mon, 26 Jan 2009 10:00:40 GMT", //
      "Connection", "close", //
      "Content-Length", "16378" };
  public final static String testHeaderString1;
  static {
    StringBuilder headers = new StringBuilder();
    headers.append(statusLine1).append(WarcRecordWriter.CRLF);
    for (int i = 0; i < testHeaders1.length; i += 2) {
      headers.append(testHeaders1[i]).append(WarcRecordWriter.COLONSP);
      headers.append(testHeaders1[i+1]).append(WarcRecordWriter.CRLF);
    }
    testHeaderString1 = headers.toString();
  }

  @Test
  public void testFormatHttpHeaders() {
    assertEquals("Formatting HTTP header failed", testHeaderString1,
        WarcRecordWriter.formatHttpHeaders(statusLine1,
            Arrays.asList(testHeaders1)));
  }

  @Test
  public void testFixHttpHeaders() {
    StringBuilder headers = new StringBuilder();
    headers.append(statusLine1).append(WarcRecordWriter.CRLF);
    for (int i = 0; i < testHeaders1.length; i += 2) {
      headers.append(testHeaders1[i]).append(WarcRecordWriter.COLONSP);
      headers.append(testHeaders1[i+1]).append(WarcRecordWriter.CRLF);
    }
    String fixed = WarcRecordWriter.fixHttpHeaders(WarcRecordWriter.formatHttpHeaders(statusLine1,
        Arrays.asList(testHeaders1)), 50000);
    assertFalse("Content-Encoding should be removed",
        fixed.contains("\r\nContent-Encoding:"));
//    assertFalse("Transfer-Encoding should be removed",
//        fixed.contains("\r\nTransfer-Encoding:"));
    assertTrue("Content-Length to be fixed",
        fixed.contains("\r\nContent-Length: 50000\r\n"));
  }
}
