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

package org.apache.nutch.protocol;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.WritableTestUtils;
import org.apache.tika.mime.MimeTypes;

import junit.framework.TestCase;


/** Unit tests for Content. */

public class TestContent extends TestCase {

  private static Configuration conf = NutchConfiguration.create();

  public TestContent(String name) { super(name); }

  public void testContent() throws Exception {

    String page = "<HTML><BODY><H1>Hello World</H1><P>The Quick Brown Fox Jumped Over the Lazy Fox.</BODY></HTML>";

    String url = "http://www.foo.com/";

    SpellCheckedMetadata metaData = new SpellCheckedMetadata();
    metaData.add("Host", "www.foo.com");
    metaData.add("Content-Type", "text/html");

    Content r = new Content(url, url, page.getBytes("UTF8"), "text/html",
                            metaData, conf);

    WritableTestUtils.testWritable(r);
    assertEquals("text/html", r.getMetadata().get("Content-Type"));
    assertEquals("text/html", r.getMetadata().get("content-type"));
    assertEquals("text/html", r.getMetadata().get("CONTENTYPE"));
  }

  /** Unit tests for getContentType(String, String, byte[]) method. */
  public void testGetContentType() throws Exception {
    Content c = null;
    Metadata p = new Metadata();

    c = new Content("http://www.foo.com/",
                    "http://www.foo.com/",
                    "".getBytes("UTF8"),
                    "text/html; charset=UTF-8", p, conf);
    assertEquals("text/html", c.getContentType());

    c = new Content("http://www.foo.com/foo.html",
                    "http://www.foo.com/",
                    "".getBytes("UTF8"),
                    "", p, conf);
    assertEquals("text/html", c.getContentType());

    c = new Content("http://www.foo.com/foo.html",
                    "http://www.foo.com/",
                    "".getBytes("UTF8"),
                    null, p, conf);
    assertEquals("text/html", c.getContentType());

    c = new Content("http://www.foo.com/",
                    "http://www.foo.com/",
                    "<html></html>".getBytes("UTF8"),
                    "", p, conf);
    assertEquals("text/html", c.getContentType());

    c = new Content("http://www.foo.com/foo.html",
                    "http://www.foo.com/",
                    "<html></html>".getBytes("UTF8"),
                    "text/plain", p, conf);
    assertEquals("text/html", c.getContentType());

    c = new Content("http://www.foo.com/foo.png",
                    "http://www.foo.com/",
                    "<html></html>".getBytes("UTF8"),
                    "text/plain", p, conf);
    assertEquals("text/html", c.getContentType());

    c = new Content("http://www.foo.com/",
                    "http://www.foo.com/",
                    "".getBytes("UTF8"),
                    "", p, conf);
    assertEquals(MimeTypes.DEFAULT, c.getContentType());

    c = new Content("http://www.foo.com/",
                    "http://www.foo.com/",
                    "".getBytes("UTF8"),
                    null, p, conf);
    assertNotNull(c.getContentType());
  }

}
