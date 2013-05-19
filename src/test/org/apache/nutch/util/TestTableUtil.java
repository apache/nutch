/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.util;

import org.apache.nutch.util.TableUtil;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestTableUtil {

  String urlString1 = "http://foo.com/";
  String urlString2 = "http://foo.com:8900/";
  String urlString3 = "ftp://bar.baz.com/";
  String urlString4 = "http://bar.baz.com:8983/to/index.html?a=b&c=d";
  String urlString5 = "http://foo.com?a=/a/b&c=0";
  String urlString5rev = "http://foo.com/?a=/a/b&c=0";
  String urlString6 = "http://foo.com";
  String urlString7 = "file:///var/www/index.html";

  String reversedUrlString1 = "com.foo:http/";
  String reversedUrlString2 = "com.foo:http:8900/";
  String reversedUrlString3 = "com.baz.bar:ftp/";
  String reversedUrlString4 = "com.baz.bar:http:8983/to/index.html?a=b&c=d";
  String reversedUrlString5 = "com.foo:http/?a=/a/b&c=0";
  String reversedUrlString6 = "com.foo:http";
  String reversedUrlString7 = ":file/var/www/index.html";

  @Test
  public void testReverseUrl() throws Exception {
    assertReverse(urlString1, reversedUrlString1);
    assertReverse(urlString2, reversedUrlString2);
    assertReverse(urlString3, reversedUrlString3);
    assertReverse(urlString4, reversedUrlString4); 
    assertReverse(urlString5, reversedUrlString5); 
    assertReverse(urlString5, reversedUrlString5); 
    assertReverse(urlString6, reversedUrlString6); 
    assertReverse(urlString7, reversedUrlString7);
  }

  @Test
  public void testUnreverseUrl() throws Exception {
    assertUnreverse(reversedUrlString1, urlString1);
    assertUnreverse(reversedUrlString2, urlString2);
    assertUnreverse(reversedUrlString3, urlString3);
    assertUnreverse(reversedUrlString4, urlString4);
    assertUnreverse(reversedUrlString5, urlString5rev);
    assertUnreverse(reversedUrlString6, urlString6);
    assertUnreverse(reversedUrlString7, urlString7);
  }

  private static void assertReverse(String url, String expectedReversedUrl) throws Exception {
    String reversed = TableUtil.reverseUrl(url);
    assertEquals(expectedReversedUrl, reversed);
  }

  private static void assertUnreverse(String reversedUrl, String expectedUrl) {
    String unreversed = TableUtil.unreverseUrl(reversedUrl);
    assertEquals(expectedUrl, unreversed);
  }
}
