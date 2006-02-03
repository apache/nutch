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

package org.apache.nutch.net;

import org.apache.nutch.net.RegexUrlNormalizer;

/** Unit tests for RegexUrlNormalizer. */
public class TestRegexUrlNormalizer extends TestBasicUrlNormalizer {
  public TestRegexUrlNormalizer(String name) { super(name); }

  public void testNormalizer() throws Exception {
    normalizeTest("http://foo.com/foo.php?f=2&PHPSESSID=cdc993a493e899bed04f4d0c8a462a03",
      "http://foo.com/foo.php?f=2");
    normalizeTest("http://foo.com/foo.php?f=2&PHPSESSID=cdc993a493e899bed04f4d0c8a462a03&q=3",
      "http://foo.com/foo.php?f=2&q=3");
    normalizeTest("http://foo.com/foo.php?PHPSESSID=cdc993a493e899bed04f4d0c8a462a03&f=2",
      "http://foo.com/foo.php?f=2");
    normalizeTest("http://foo.com/foo.php?PHPSESSID=cdc993a493e899bed04f4d0c8a462a03",
      "http://foo.com/foo.php");
  }

  private void normalizeTest(String weird, String normal) throws Exception {
    String testSrcDir = System.getProperty("test.src.dir");
    String path = testSrcDir + "/org/apache/nutch/net/test-regex-normalize.xml";
    RegexUrlNormalizer normalizer = new RegexUrlNormalizer(path);
    assertEquals(normal, normalizer.normalize(weird));
  }
	
  public static void main(String[] args) throws Exception {
    new TestRegexUrlNormalizer("test").testNormalizer();
    new TestBasicUrlNormalizer("test").testNormalizer(); // need to make sure it passes this test too
  }



}
