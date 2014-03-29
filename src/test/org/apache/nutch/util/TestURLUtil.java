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

import org.junit.Assert;
import org.junit.Test;

/** Test class for URLUtil */
public class TestURLUtil {

  @Test
  public void testGetDomainName()
    throws Exception {

    URL url = null;

    url = new URL("http://lucene.apache.org/nutch");
    Assert.assertEquals("apache.org", URLUtil.getDomainName(url));

    url = new URL("http://en.wikipedia.org/wiki/Java_coffee");
    Assert.assertEquals("wikipedia.org", URLUtil.getDomainName(url));

    url = new URL("http://140.211.11.130/foundation/contributing.html");
    Assert.assertEquals("140.211.11.130", URLUtil.getDomainName(url));

    url = new URL("http://www.example.co.uk:8080/index.html");
    Assert.assertEquals("example.co.uk", URLUtil.getDomainName(url));

    url = new URL("http://com");
    Assert.assertEquals("com", URLUtil.getDomainName(url));

    url = new URL("http://www.example.co.uk.com");
    Assert.assertEquals("uk.com", URLUtil.getDomainName(url));

    // "nn" is not a tld
    url = new URL("http://example.com.nn");
    Assert.assertEquals("nn", URLUtil.getDomainName(url));

    url = new URL("http://");
    Assert.assertEquals("", URLUtil.getDomainName(url));

    url = new URL("http://www.edu.tr.xyz");
    Assert.assertEquals("xyz", URLUtil.getDomainName(url));

    url = new URL("http://www.example.c.se");
    Assert.assertEquals("example.c.se", URLUtil.getDomainName(url));

    // plc.co.im is listed as a domain suffix
    url = new URL("http://www.example.plc.co.im");
    Assert.assertEquals("example.plc.co.im", URLUtil.getDomainName(url));

    // 2000.hu is listed as a domain suffix
    url = new URL("http://www.example.2000.hu");
    Assert.assertEquals("example.2000.hu", URLUtil.getDomainName(url));

    // test non-ascii
    url = new URL("http://www.example.商業.tw");
    Assert.assertEquals("example.商業.tw", URLUtil.getDomainName(url));
  }

  @Test
  public void testGetDomainSuffix()
    throws Exception {
    URL url = null;

    url = new URL("http://lucene.apache.org/nutch");
    Assert.assertEquals("org", URLUtil.getDomainSuffix(url).getDomain());

    url = new URL("http://140.211.11.130/foundation/contributing.html");
    Assert.assertNull(URLUtil.getDomainSuffix(url));

    url = new URL("http://www.example.co.uk:8080/index.html");
    Assert.assertEquals("co.uk", URLUtil.getDomainSuffix(url).getDomain());

    url = new URL("http://com");
    Assert.assertEquals("com", URLUtil.getDomainSuffix(url).getDomain());

    url = new URL("http://www.example.co.uk.com");
    Assert.assertEquals("com", URLUtil.getDomainSuffix(url).getDomain());

    // "nn" is not a tld
    url = new URL("http://example.com.nn");
    Assert.assertNull(URLUtil.getDomainSuffix(url));

    url = new URL("http://");
    Assert.assertNull(URLUtil.getDomainSuffix(url));

    url = new URL("http://www.edu.tr.xyz");
    Assert.assertNull(URLUtil.getDomainSuffix(url));

    url = new URL("http://subdomain.example.edu.tr");
    Assert.assertEquals("edu.tr", URLUtil.getDomainSuffix(url).getDomain());

    url = new URL("http://subdomain.example.presse.fr");
    Assert.assertEquals("presse.fr", URLUtil.getDomainSuffix(url).getDomain());

    url = new URL("http://subdomain.example.presse.tr");
    Assert.assertEquals("tr", URLUtil.getDomainSuffix(url).getDomain());

    // plc.co.im is listed as a domain suffix
    url = new URL("http://www.example.plc.co.im");
    Assert.assertEquals("plc.co.im", URLUtil.getDomainSuffix(url).getDomain());

    // 2000.hu is listed as a domain suffix
    url = new URL("http://www.example.2000.hu");
    Assert.assertEquals("2000.hu", URLUtil.getDomainSuffix(url).getDomain());

    // test non-ascii
    url = new URL("http://www.example.商業.tw");
    Assert.assertEquals("商業.tw", URLUtil.getDomainSuffix(url).getDomain());
  }

  @Test
  public void testGetHostSegments()
    throws Exception {
    URL url;
    String[] segments;

    url = new URL("http://subdomain.example.edu.tr");
    segments = URLUtil.getHostSegments(url);
    Assert.assertEquals("subdomain", segments[0]);
    Assert.assertEquals("example", segments[1]);
    Assert.assertEquals("edu", segments[2]);
    Assert.assertEquals("tr", segments[3]);

    url = new URL("http://");
    segments = URLUtil.getHostSegments(url);
    Assert.assertEquals(1, segments.length);
    Assert.assertEquals("", segments[0]);

    url = new URL("http://140.211.11.130/foundation/contributing.html");
    segments = URLUtil.getHostSegments(url);
    Assert.assertEquals(1, segments.length);
    Assert.assertEquals("140.211.11.130", segments[0]);

    // test non-ascii
    url = new URL("http://www.example.商業.tw");
    segments = URLUtil.getHostSegments(url);
    Assert.assertEquals("www", segments[0]);
    Assert.assertEquals("example", segments[1]);
    Assert.assertEquals("商業", segments[2]);
    Assert.assertEquals("tw", segments[3]);

  }

  @Test
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
    Assert.assertEquals(bDotCom, URLUtil.chooseRepr(aDotCom, bDotCom, true));
    Assert.assertEquals(bDotCom, URLUtil.chooseRepr(aDotCom, bDotCom, false));
    
    // 2) permanent and root, keep src
    // *a.com -> a.com?y=1 || *a.com -> a.com/xyz/index.html
    Assert.assertEquals(aDotCom, URLUtil.chooseRepr(aDotCom, aQStr, false));
    Assert.assertEquals(aDotCom, URLUtil.chooseRepr(aDotCom, aPath, false));
    
    //3) permanent and not root and dest root, keep dest
    //a.com/xyz/index.html -> a.com*
    Assert.assertEquals(aDotCom, URLUtil.chooseRepr(aPath, aDotCom, false));
    
    //4) permanent and neither root keep dest
    // a.com/xyz/index.html -> a.com/abc/page.html*
    Assert.assertEquals(aPath2, URLUtil.chooseRepr(aPath, aPath2, false));
    
    //5) temp and root and dest not root keep src
    //*a.com -> a.com/xyz/index.html
    Assert.assertEquals(aDotCom, URLUtil.chooseRepr(aDotCom, aPath, true));
    
    //6) temp and not root and dest root keep dest
    // a.com/xyz/index.html -> a.com*
    Assert.assertEquals(aDotCom, URLUtil.chooseRepr(aPath, aDotCom, true));

    //7) temp and neither root, keep shortest, if hosts equal by path else by hosts
    //  a.com/xyz/index.html -> a.com/abc/page.html*
    // *www.a.com/xyz/index.html -> www.news.a.com/xyz/index.html
    Assert.assertEquals(aPath2, URLUtil.chooseRepr(aPath, aPath2, true));
    Assert.assertEquals(aPath, URLUtil.chooseRepr(aPath, aPath3, true));

    //8) temp and both root keep shortest sub domain
    // *www.a.com -> www.news.a.com
    Assert.assertEquals(aDotCom, URLUtil.chooseRepr(aDotCom, aSubDotCom, true));
  }

  // from RFC3986 section 5.4.1
  private static String baseString = "http://a/b/c/d;p?q";
  private static String[][] targets = new String[][] {
    // unknown protocol {"g:h"           ,  "g:h"},
    {"g"             ,  "http://a/b/c/g"},
    { "./g"           ,  "http://a/b/c/g"},
    { "g/"            ,  "http://a/b/c/g/"},
    { "/g"            ,  "http://a/g"},
    { "//g"           ,  "http://g"},
    { "?y"            ,  "http://a/b/c/d;p?y"},
    { "g?y"           ,  "http://a/b/c/g?y"},
    { "#s"            ,  "http://a/b/c/d;p?q#s"},
    { "g#s"           ,  "http://a/b/c/g#s"},
    { "g?y#s"         ,  "http://a/b/c/g?y#s"},
    { ";x"            ,  "http://a/b/c/;x"},
    { "g;x"           ,  "http://a/b/c/g;x"},
    { "g;x?y#s"       ,  "http://a/b/c/g;x?y#s"},
    { ""              ,  "http://a/b/c/d;p?q"},
    { "."             ,  "http://a/b/c/"},
    { "./"            ,  "http://a/b/c/"},
    { ".."            ,  "http://a/b/"},
    { "../"           ,  "http://a/b/"},
    { "../g"          ,  "http://a/b/g"},
    { "../.."         ,  "http://a/"},
    { "../../"        ,  "http://a/"},
    { "../../g"       ,  "http://a/g"}
  };

  @Test
  public void testResolveURL() throws Exception {
    // test NUTCH-436
    URL u436 = new URL("http://a/b/c/d;p?q#f");
    Assert.assertEquals("http://a/b/c/d;p?q#f", u436.toString());
    URL abs = URLUtil.resolveURL(u436, "?y");
    Assert.assertEquals("http://a/b/c/d;p?y", abs.toString());
    // test NUTCH-566
    URL u566 = new URL("http://www.fleurie.org/entreprise.asp");
    abs = URLUtil.resolveURL(u566, "?id_entrep=111");
    Assert.assertEquals("http://www.fleurie.org/entreprise.asp?id_entrep=111", abs.toString());
    URL base = new URL(baseString);
    Assert.assertEquals("base url parsing", baseString, base.toString());
    for (int i = 0; i < targets.length; i++) {
      URL u = URLUtil.resolveURL(base, targets[i][0]);
      Assert.assertEquals(targets[i][1], targets[i][1], u.toString());
    }
  }
  
  @Test
  public void testToUNICODE() throws Exception {
    Assert.assertEquals("http://www.çevir.com", URLUtil.toUNICODE("http://www.xn--evir-zoa.com"));
    Assert.assertEquals("http://uni-tübingen.de/", URLUtil.toUNICODE("http://xn--uni-tbingen-xhb.de/"));
    Assert.assertEquals(
        "http://www.medizin.uni-tübingen.de:8080/search.php?q=abc#p1",
        URLUtil.toUNICODE("http://www.medizin.xn--uni-tbingen-xhb.de:8080/search.php?q=abc#p1"));
    
  }
  
  @Test
  public void testToASCII() throws Exception {
    Assert.assertEquals("http://www.xn--evir-zoa.com", URLUtil.toASCII("http://www.çevir.com"));
    Assert.assertEquals("http://xn--uni-tbingen-xhb.de/", URLUtil.toASCII("http://uni-tübingen.de/"));
    Assert.assertEquals(
        "http://www.medizin.xn--uni-tbingen-xhb.de:8080/search.php?q=abc#p1",
        URLUtil.toASCII("http://www.medizin.uni-tübingen.de:8080/search.php?q=abc#p1")); 
  }

}
