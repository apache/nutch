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

package org.apache.nutch.pagedb;

import java.io.*;
import java.util.Random;
import org.apache.nutch.io.*;
import org.apache.nutch.db.*;
import junit.framework.TestCase;

/** Unit tests for Page. */
public class TestPage extends TestCase {
  public TestPage(String name) { super(name); }

  public static Page getTestPage() throws Exception {
    return new Page("http://foo.com/", TestMD5Hash.getTestHash());
  }

  public void testPage() throws Exception {
    TestWritable.testWritable(getTestPage());
  }

  public void testPageCtors() throws Exception {
    Random random = new Random();
    String urlString = "http://foo.com/";
    MD5Hash md5 = MD5Hash.digest(urlString);
    long now = System.currentTimeMillis();
    float score = random.nextFloat();
    float nextScore = random.nextFloat();

    Page page = new Page(urlString, md5);
    assertEquals(page.getURL().toString(), urlString);
    assertEquals(page.getMD5(), md5);

    page = new Page(urlString, score);
    assertEquals(page.getURL().toString(), urlString);
    assertEquals(page.getMD5(), md5);
    assertEquals(page.getScore(), score, 0.0f);
    assertEquals(page.getNextScore(), score, 0.0f);

    page = new Page(urlString, score, now);
    assertEquals(page.getURL().toString(), urlString);
    assertEquals(page.getMD5(), md5);
    assertEquals(page.getNextFetchTime(), now);
    assertEquals(page.getScore(), score, 0.0f);
    assertEquals(page.getNextScore(), score, 0.0f);

    page = new Page(urlString, score, nextScore, now);
    assertEquals(page.getURL().toString(), urlString);
    assertEquals(page.getMD5(), md5);
    assertEquals(page.getNextFetchTime(), now);
    assertEquals(page.getScore(), score, 0.0f);
    assertEquals(page.getNextScore(), nextScore, 0.0f);
  }

}
