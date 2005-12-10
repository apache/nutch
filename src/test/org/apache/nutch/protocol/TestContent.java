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

package org.apache.nutch.protocol;

import java.io.*;
import java.util.Properties;
import org.apache.nutch.io.*;
import org.apache.nutch.pagedb.*;
import junit.framework.TestCase;

/** Unit tests for Content. */

public class TestContent extends TestCase {
  public TestContent(String name) { super(name); }

  public void testContent() throws Exception {

    String page = "<HTML><BODY><H1>Hello World</H1><P>The Quick Brown Fox Jumped Over the Lazy Fox.</BODY></HTML>";

    String url = "http://www.foo.com/";

    Properties metaData = new Properties();
    metaData.put("Host", "www.foo.com");
    metaData.put("Content-Type", "text/html");

    Content r = new Content(url, url, page.getBytes("UTF8"), "text/html",
                            metaData);
                        
    TestWritable.testWritable(r);
  }

  /** Unit tests for getContentType(String, String, byte[]) method. */
  public void testGetContentType() throws Exception {
    Content c = null;
    Properties p = new Properties();

    c = new Content("http://www.foo.com/",
                    "http://www.foo.com/",
                    "".getBytes("UTF8"),
                    "text/html; charset=UTF-8", p);
    assertEquals("text/html", c.getContentType());

    c = new Content("http://www.foo.com/foo.html",
                    "http://www.foo.com/",
                    "".getBytes("UTF8"),
                    "", p);
    assertEquals("text/html", c.getContentType());

    c = new Content("http://www.foo.com/foo.html",
                    "http://www.foo.com/",
                    "".getBytes("UTF8"),
                    null, p);
    assertEquals("text/html", c.getContentType());

    c = new Content("http://www.foo.com/",
                    "http://www.foo.com/",
                    "<html></html>".getBytes("UTF8"),
                    "", p);
    assertEquals("text/html", c.getContentType());

    c = new Content("http://www.foo.com/foo.html",
                    "http://www.foo.com/",
                    "<html></html>".getBytes("UTF8"),
                    "text/plain", p);
    assertEquals("text/html", c.getContentType());

    c = new Content("http://www.foo.com/foo.png",
                    "http://www.foo.com/",
                    "<html></html>".getBytes("UTF8"),
                    "text/plain", p);
    assertEquals("text/html", c.getContentType());

    c = new Content("http://www.foo.com/",
                    "http://www.foo.com/",
                    "".getBytes("UTF8"),
                    "", p);
    assertEquals("", c.getContentType());

    c = new Content("http://www.foo.com/",
                    "http://www.foo.com/",
                    "".getBytes("UTF8"),
                    null, p);
    assertNull(c.getContentType());
  }
	
}
