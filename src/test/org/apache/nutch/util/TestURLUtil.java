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

import java.net.URL;

import junit.framework.TestCase;

/** Test class for URLUtil */
public class TestURLUtil
  extends TestCase {

  @Override
  protected void setUp()
    throws Exception {
    super.setUp();
  }

  public void testGetDomainName()
    throws Exception {

    URL url = null;

    url = new URL("http://lucene.apache.org/nutch");
    assertEquals("apache.org", URLUtil.getDomainName(url));

    url = new URL("http://en.wikipedia.org/wiki/Java_coffee");
    assertEquals("wikipedia.org", URLUtil.getDomainName(url));

    url = new URL("http://140.211.11.130/foundation/contributing.html");
    assertEquals("140.211.11.130", URLUtil.getDomainName(url));

    url = new URL("http://www.example.co.uk:8080/index.html");
    assertEquals("example.co.uk", URLUtil.getDomainName(url));

    url = new URL("http://com");
    assertEquals("com", URLUtil.getDomainName(url));

    url = new URL("http://www.example.co.uk.com");
    assertEquals("uk.com", URLUtil.getDomainName(url));

    // "nn" is not a tld
    url = new URL("http://example.com.nn");
    assertEquals("nn", URLUtil.getDomainName(url));

    url = new URL("http://");
    assertEquals("", URLUtil.getDomainName(url));

    url = new URL("http://www.edu.tr.xyz");
    assertEquals("xyz", URLUtil.getDomainName(url));

    url = new URL("http://www.example.c.se");
    assertEquals("example.c.se", URLUtil.getDomainName(url));

    // plc.co.im is listed as a domain suffix
    url = new URL("http://www.example.plc.co.im");
    assertEquals("example.plc.co.im", URLUtil.getDomainName(url));

    // 2000.hu is listed as a domain suffix
    url = new URL("http://www.example.2000.hu");
    assertEquals("example.2000.hu", URLUtil.getDomainName(url));

    // test non-ascii
    url = new URL("http://www.example.商業.tw");
    assertEquals("example.商業.tw", URLUtil.getDomainName(url));

  }

  public void testGetDomainSuffix()
    throws Exception {
    URL url = null;

    url = new URL("http://lucene.apache.org/nutch");
    assertEquals("org", URLUtil.getDomainSuffix(url).getDomain());

    url = new URL("http://140.211.11.130/foundation/contributing.html");
    assertNull(URLUtil.getDomainSuffix(url));

    url = new URL("http://www.example.co.uk:8080/index.html");
    assertEquals("co.uk", URLUtil.getDomainSuffix(url).getDomain());

    url = new URL("http://com");
    assertEquals("com", URLUtil.getDomainSuffix(url).getDomain());

    url = new URL("http://www.example.co.uk.com");
    assertEquals("com", URLUtil.getDomainSuffix(url).getDomain());

    // "nn" is not a tld
    url = new URL("http://example.com.nn");
    assertNull(URLUtil.getDomainSuffix(url));

    url = new URL("http://");
    assertNull(URLUtil.getDomainSuffix(url));

    url = new URL("http://www.edu.tr.xyz");
    assertNull(URLUtil.getDomainSuffix(url));

    url = new URL("http://subdomain.example.edu.tr");
    assertEquals("edu.tr", URLUtil.getDomainSuffix(url).getDomain());

    url = new URL("http://subdomain.example.presse.fr");
    assertEquals("presse.fr", URLUtil.getDomainSuffix(url).getDomain());

    url = new URL("http://subdomain.example.presse.tr");
    assertEquals("tr", URLUtil.getDomainSuffix(url).getDomain());

    // plc.co.im is listed as a domain suffix
    url = new URL("http://www.example.plc.co.im");
    assertEquals("plc.co.im", URLUtil.getDomainSuffix(url).getDomain());

    // 2000.hu is listed as a domain suffix
    url = new URL("http://www.example.2000.hu");
    assertEquals("2000.hu", URLUtil.getDomainSuffix(url).getDomain());

    // test non-ascii
    url = new URL("http://www.example.商業.tw");
    assertEquals("商業.tw", URLUtil.getDomainSuffix(url).getDomain());

  }

  public void testGetHostSegments()
    throws Exception {
    URL url;
    String[] segments;

    url = new URL("http://subdomain.example.edu.tr");
    segments = URLUtil.getHostSegments(url);
    assertEquals("subdomain", segments[0]);
    assertEquals("example", segments[1]);
    assertEquals("edu", segments[2]);
    assertEquals("tr", segments[3]);

    url = new URL("http://");
    segments = URLUtil.getHostSegments(url);
    assertEquals(1, segments.length);
    assertEquals("", segments[0]);

    url = new URL("http://140.211.11.130/foundation/contributing.html");
    segments = URLUtil.getHostSegments(url);
    assertEquals(1, segments.length);
    assertEquals("140.211.11.130", segments[0]);

    // test non-ascii
    url = new URL("http://www.example.商業.tw");
    segments = URLUtil.getHostSegments(url);
    assertEquals("www", segments[0]);
    assertEquals("example", segments[1]);
    assertEquals("商業", segments[2]);
    assertEquals("tw", segments[3]);

  }

  public void testChooseRepr()
    throws Exception {
    
    String aDotCom = "http://www.a.com";
    String bDotCom = "http://www.b.com";
    String aSubDotCom = "http://www.news.a.com";
    String aQStr = "http://www.a.com?y=1";
    String aPath = "http://www.a.com/xyz/index.html";
    String aPath2 = "http://www.a.com/abc/page.html";
    String aPath3 = "http://www.news.a.com/abc/page.html";
    
    // 1) different domain them keep dest, temp or perm
    // a.com -> b.com*
    assertEquals(bDotCom, URLUtil.chooseRepr(aDotCom, bDotCom, true));
    assertEquals(bDotCom, URLUtil.chooseRepr(aDotCom, bDotCom, false));
    
    // 2) permanent and root, keep src
    // *a.com -> a.com?y=1 || *a.com -> a.com/xyz/index.html
    assertEquals(aDotCom, URLUtil.chooseRepr(aDotCom, aQStr, false));
    assertEquals(aDotCom, URLUtil.chooseRepr(aDotCom, aPath, false));
    
    //3) permanent and not root and dest root, keep dest
    //a.com/xyz/index.html -> a.com*
    assertEquals(aDotCom, URLUtil.chooseRepr(aPath, aDotCom, false));
    
    //4) permanent and neither root keep dest
    // a.com/xyz/index.html -> a.com/abc/page.html*
    assertEquals(aPath2, URLUtil.chooseRepr(aPath, aPath2, false));
    
    //5) temp and root and dest not root keep src
    //*a.com -> a.com/xyz/index.html
    assertEquals(aDotCom, URLUtil.chooseRepr(aDotCom, aPath, true));
    
    //6) temp and not root and dest root keep dest
    // a.com/xyz/index.html -> a.com*
    assertEquals(aDotCom, URLUtil.chooseRepr(aPath, aDotCom, true));

    //7) temp and neither root, keep shortest, if hosts equal by path else by hosts
    //  a.com/xyz/index.html -> a.com/abc/page.html*
    // *www.a.com/xyz/index.html -> www.news.a.com/xyz/index.html
    assertEquals(aPath2, URLUtil.chooseRepr(aPath, aPath2, true));
    assertEquals(aPath, URLUtil.chooseRepr(aPath, aPath3, true));

    //8) temp and both root keep shortest sub domain
    // *www.a.com -> www.news.a.com
    assertEquals(aDotCom, URLUtil.chooseRepr(aDotCom, aSubDotCom, true));
  }

}
