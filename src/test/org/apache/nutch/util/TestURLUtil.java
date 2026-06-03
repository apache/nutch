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
package org.apache.nutch.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Locale;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Test class for URLUtil */
public class TestURLUtil {

  @Test
  public void testGetDomainName() throws Exception {

    URL url = null;

    url = new URL("http://lucene.apache.org/nutch");
    assertEquals("apache.org", URLUtil.getDomainName(url));

    // hostname with trailing dot
    url = new URL("https://lucene.apache.org./nutch");
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

    // "nn" is not a public suffix
    url = new URL("http://example.com.nn");
    assertEquals("example.com.nn", URLUtil.getDomainName(url));

    url = new URL("http://");
    assertEquals("", URLUtil.getDomainName(url));

    /*
     * "xyz" is an ICANN suffix since 2014, see
     * https://www.iana.org/domains/root/db/xyz.html
     */
    url = new URL("http://www.edu.tr.xyz");
    assertEquals("tr.xyz", URLUtil.getDomainName(url));

    url = new URL("http://www.example.c.se");
    assertEquals("example.c.se", URLUtil.getDomainName(url));

    // plc.co.im is listed as a domain suffix
    url = new URL("http://www.example.plc.co.im");
    assertEquals("example.plc.co.im", URLUtil.getDomainName(url));

    // 2000.hu is listed as a domain suffix
    url = new URL("http://www.example.2000.hu");
    assertEquals("example.2000.hu", URLUtil.getDomainName(url));

    // test non-ascii
    url = new URL("http://www.example.flå.no");
    assertEquals("example.flå.no", URLUtil.getDomainName(url));
    url = new URL("http://www.example.栃木.jp");
    assertEquals("example.栃木.jp", URLUtil.getDomainName(url));
    // broken by https://github.com/publicsuffix/list/commit/408a7b0bdec993884865baaa2f0d14cc9a060885
    // url = new URL("http://www.example.商業.tw");
    // Assert.assertEquals("example.商業.tw", URLUtil.getDomainName(url));

    // test URL without host/authority
    url = new URL("file:/path/index.html");
    assertNotNull(URLUtil.getDomainName(url));
    assertEquals("", URLUtil.getDomainName(url));
  }

  @Test
  public void testGetDomainSuffix() throws Exception {
    URL url = null;

    url = new URL("http://lucene.apache.org/nutch");
    assertEquals("org", URLUtil.getDomainSuffix(url));

    // hostname with trailing dot
    url = new URL("https://lucene.apache.org./nutch");
    assertEquals("org", URLUtil.getDomainSuffix(url));

    url = new URL("http://140.211.11.130/foundation/contributing.html");
    assertNull(URLUtil.getDomainSuffix(url));

    url = new URL("http://www.example.co.uk:8080/index.html");
    assertEquals("co.uk", URLUtil.getDomainSuffix(url));

    url = new URL("http://com");
    assertEquals("com", URLUtil.getDomainSuffix(url));

    url = new URL("http://www.example.co.uk.com");
    assertEquals("com", URLUtil.getDomainSuffix(url));

    // "nn" is not a public suffix
    url = new URL("http://example.com.nn");
    assertNull(URLUtil.getDomainSuffix(url));

    url = new URL("http://");
    assertNull(URLUtil.getDomainSuffix(url));

    /*
     * "xyz" is an ICANN suffix since 2014, see
     * https://www.iana.org/domains/root/db/xyz.html
     */
    url = new URL("http://www.edu.tr.xyz");
    assertEquals("xyz", URLUtil.getDomainSuffix(url));

    url = new URL("http://subdomain.example.edu.tr");
    assertEquals("edu.tr", URLUtil.getDomainSuffix(url));

    url = new URL("http://subdomain.example.presse.fr");
    assertEquals("fr", URLUtil.getDomainSuffix(url));

    url = new URL("http://subdomain.example.presse.tr");
    assertEquals("tr", URLUtil.getDomainSuffix(url));

    // plc.co.im is listed as a domain suffix
    url = new URL("http://www.example.plc.co.im");
    assertEquals("plc.co.im", URLUtil.getDomainSuffix(url));

    // 2000.hu is listed as a domain suffix
    url = new URL("http://www.example.2000.hu");
    assertEquals("2000.hu", URLUtil.getDomainSuffix(url));

    // test non-ASCII
    url = new URL("https://www.taiuru.māori.nz/");
    assertEquals("xn--mori-qsa.nz", URLUtil.getDomainSuffix(url));
    url = new URL("http://www.example.flå.no");
    assertEquals("xn--fl-zia.no", URLUtil.getDomainSuffix(url));
    url = new URL("http://www.example.栃木.jp");
    assertEquals("xn--4pvxs.jp", URLUtil.getDomainSuffix(url));
    // broken by https://github.com/publicsuffix/list/commit/408a7b0bdec993884865baaa2f0d14cc9a060885
    // url = new URL("http://www.example.商業.tw");
    // assertEquals("xn--czrw28b.tw", URLUtil.getDomainSuffix(url));
  }

  @Test
  public void testGetTopLevelDomain() throws Exception {
    URL url = null;

    url = new URL("http://lucene.apache.org/nutch");
    assertEquals("org", URLUtil.getTopLevelDomainName(url));

    // hostname with trailing dot
    url = new URL("https://lucene.apache.org./nutch");
    assertEquals("org", URLUtil.getTopLevelDomainName(url));

    url = new URL("http://140.211.11.130/foundation/contributing.html");
    assertNull(URLUtil.getTopLevelDomainName(url));

    url = new URL("http://www.example.co.uk:8080/index.html");
    assertEquals("uk", URLUtil.getTopLevelDomainName(url));

    // "nn" is not a public suffix
    url = new URL("http://example.com.nn");
    assertNull(URLUtil.getTopLevelDomainName(url));

    url = new URL("http://");
    assertNull(URLUtil.getTopLevelDomainName(url));

    url = new URL("http://nic.삼성/");
    assertEquals("xn--cg4bki", URLUtil.getTopLevelDomainName(url));
  }

  @Test
  public void testGetHostSegments() throws Exception {
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

  @Test
  public void testChooseRepr() throws Exception {

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

    // 3) permanent and not root and dest root, keep dest
    // a.com/xyz/index.html -> a.com*
    assertEquals(aDotCom, URLUtil.chooseRepr(aPath, aDotCom, false));

    // 4) permanent and neither root keep dest
    // a.com/xyz/index.html -> a.com/abc/page.html*
    assertEquals(aPath2, URLUtil.chooseRepr(aPath, aPath2, false));

    // 5) temp and root and dest not root keep src
    // *a.com -> a.com/xyz/index.html
    assertEquals(aDotCom, URLUtil.chooseRepr(aDotCom, aPath, true));

    // 6) temp and not root and dest root keep dest
    // a.com/xyz/index.html -> a.com*
    assertEquals(aDotCom, URLUtil.chooseRepr(aPath, aDotCom, true));

    // 7) temp and neither root, keep shortest, if hosts equal by path else by
    // hosts
    // a.com/xyz/index.html -> a.com/abc/page.html*
    // *www.a.com/xyz/index.html -> www.news.a.com/xyz/index.html
    assertEquals(aPath2, URLUtil.chooseRepr(aPath, aPath2, true));
    assertEquals(aPath, URLUtil.chooseRepr(aPath, aPath3, true));

    // 8) temp and both root keep shortest sub domain
    // *www.a.com -> www.news.a.com
    assertEquals(aDotCom, URLUtil.chooseRepr(aDotCom, aSubDotCom, true));
  }

  // from RFC3986 section 5.4.1
  private static String baseString = "http://a/b/c/d;p?q";
  private static String[][] targets = new String[][] {
      // unknown protocol {"g:h" , "g:h"},
      { "g", "http://a/b/c/g" }, { "./g", "http://a/b/c/g" },
      { "g/", "http://a/b/c/g/" }, { "/g", "http://a/g" },
      { "//g", "http://g" }, { "?y", "http://a/b/c/d;p?y" },
      { "g?y", "http://a/b/c/g?y" }, { "#s", "http://a/b/c/d;p?q#s" },
      { "g#s", "http://a/b/c/g#s" }, { "g?y#s", "http://a/b/c/g?y#s" },
      { ";x", "http://a/b/c/;x" }, { "g;x", "http://a/b/c/g;x" },
      { "g;x?y#s", "http://a/b/c/g;x?y#s" }, { "", "http://a/b/c/d;p?q" },
      { ".", "http://a/b/c/" }, { "./", "http://a/b/c/" },
      { "..", "http://a/b/" }, { "../", "http://a/b/" },
      { "../g", "http://a/b/g" }, { "../..", "http://a/" },
      { "../../", "http://a/" }, { "../../g", "http://a/g" } };

  @Test
  public void testResolveURL() throws Exception {
    // test NUTCH-436
    URL u436 = new URL("http://a/b/c/d;p?q#f");
    assertEquals("http://a/b/c/d;p?q#f", u436.toString());
    URL abs = URLUtil.resolveURL(u436, "?y");
    assertEquals("http://a/b/c/d;p?y", abs.toString());
    // test NUTCH-566
    URL u566 = new URL("http://www.fleurie.org/entreprise.asp");
    abs = URLUtil.resolveURL(u566, "?id_entrep=111");
    assertEquals("http://www.fleurie.org/entreprise.asp?id_entrep=111",
        abs.toString());
    URL base = new URL(baseString);
    assertEquals(baseString, base.toString(), "base url parsing");
    for (int i = 0; i < targets.length; i++) {
      URL u = URLUtil.resolveURL(base, targets[i][0]);
      assertEquals(targets[i][1], targets[i][1], u.toString());
    }
  }

  @Test
  public void testToUNICODE() throws Exception {
    assertEquals("http://www.çevir.com",
        URLUtil.toUNICODE("http://www.xn--evir-zoa.com"));
    assertEquals("http://uni-tübingen.de/",
        URLUtil.toUNICODE("http://xn--uni-tbingen-xhb.de/"));
    assertEquals("http://www.medizin.uni-tübingen.de:8080/search.php?q=abc#p1",
            URLUtil
                .toUNICODE("http://www.medizin.xn--uni-tbingen-xhb.de:8080/search.php?q=abc#p1"));
    // do not fail on characters not in Unicode 3.2
    assertEquals("https://example.ᬩᬮᬶ.id/",
        URLUtil.toUNICODE("https://example.xn--9tfky.id/"));
    // IDNA2008
    assertEquals("http://straße.de/",
        URLUtil.toUNICODE("http://xn--strae-oqa.de/"));
    // host names with uppercase characters
    assertEquals("https://googie.com/",
        URLUtil.toUNICODE("https://googIe.com/"));
    assertEquals("https://googie.com/", URLUtil.toASCII("https://googIe.com/"));
    assertEquals("https://xn--90ax2c.xn--p1ai/",
        URLUtil.toASCII("https://нЭб.РФ/"));
    assertEquals("https://нэб.рф/",
        URLUtil.toUNICODE("https://Xn--90Ax2c.xN--P1ai/"));
  }

  @Test
  public void testToASCII() throws Exception {
    assertEquals("http://www.xn--evir-zoa.com",
        URLUtil.toASCII("http://www.çevir.com"));
    assertEquals("http://xn--uni-tbingen-xhb.de/",
        URLUtil.toASCII("http://uni-tübingen.de/"));
    assertEquals("http://www.medizin.xn--uni-tbingen-xhb.de:8080/search.php?q=abc#p1",
            URLUtil
                .toASCII("http://www.medizin.uni-tübingen.de:8080/search.php?q=abc#p1"));
    // IDNA2003
    // assertEquals("http://strasse.de/",
    //    URLUtil.toASCII("http://straße.de/"));
    // do not fail on characters not in Unicode 3.2
    assertEquals("https://example.xn--9tfky.id/",
        URLUtil.toASCII("https://example.ᬩᬮᬶ.id/"));
    // IDNA2008
    assertEquals("http://xn--strae-oqa.de/",
        URLUtil.toASCII("http://straße.de/"));
  }

  @ParameterizedTest
  @CsvSource({ //
      "www.xn--evir-zoa.com,www.çevir.com,IDNA2003,true", //
      "xn--uni-tbingen-xhb.de,uni-tübingen.de,IDNA2003,true", //
      "example.xn--9tfky.id,example.ᬩᬮᬶ.id,IDNA2008,true", //
      // Test examples from whatwg-url
      "xn--53h.example,☕.example,IDNA2008,true", //
      "xn--0ca.xn--ssa73l,à.א̈,IDNA2008,true", //
      "xn--mgba3gch31f060k.com,\u0646\u0627\u0645\u0647\u200c\u0627\u06cc.com,IDNA2008,true", //
      /* Note: IDNA2008 and IDNA2003 deviate for the following examples,
       * cf. https://www.unicode.org/reports/tr46/#IDNA2003-Section */
      "xn--strae-oqa.de,straße.de,IDNA2008,true", //
      "strasse.de,straße.de,IDNA2003,false", //
      "strasse.de,strasse.de,IDNA2003,true", //
      "xn--fa-hia.de,faß.de,IDNA2008,true", //
      "fass.de,faß.de,IDNA2003,false", //
      "fass.de,fass.de,IDNA2003,true", //
      "xn--nxasmm1c.com,βόλος.com,IDNA2008,true", //
      "xn--nxasmq6b.com,βόλος.com,IDNA2003,false", //
      "xn--nxasmq6b.com,βόλοσ.com,IDNA2003,true", //
      "xn--10cl1a0b660p.com,ශ්‍රී.com,IDNA2008,true", //
      "xn--10cl1a0b.com,ශ්‍රී.com,IDNA2003,false", //
      "xn--10cl1a0b.com,ශ්රී.com,IDNA2003,true", //
      "xn--mgba3gch31f060k.com,نامه‌ای.com,IDNA2008,true", //
      "xn--mgba3gch31f.com,نامه‌ای.com,IDNA2003,false", //
      "xn--mgba3gch31f.com,نامهای.com,IDNA2003,true", //
      // mixed lowercase/uppercase: no round trip conversion
      "xn--bb-eka.at,ÖBB.at,IDNA2003,false", //
      "xn--bb-eka.at,öbb.at,IDNA2003,true", //
      // mixed encoding (Punycode and Unicode)
      "xn--p1ai.xn--p1ai,рф.xn--p1ai,IDNA2003,false", //
      "xn--p1ai.xn--p1ai,xn--p1ai.рф,IDNA2003,false", //
      // percent-encoding is not supported
      // "xn--p1ai.xn--p1ai,xn--p1ai.%D1%80%D1%84,IDNA2003,false", //
  })
  public final void testConvertHost(String ascii, String unicode, String type,
      boolean roundTrip) throws Exception {
    System.out.println(ascii + " <> " + unicode);
    if ("IDNA2008".equals(type)) {
      assertEquals(ascii, URLUtil.convertIDNA2008(unicode, true));
      assertEquals(unicode, URLUtil.convertIDNA2008(ascii, false));
      try {
        assertNotNull(URLUtil.convertIDNA2003(unicode, true, false));
      } catch (MalformedURLException e) {
        /*
         * Ok. A IDNA2008 input may raise an exception when using the IDNA2003
         * method
         */
      }
    } else if ("IDNA2003".equals(type)) {
      assertEquals(ascii, URLUtil.convertIDNA2003(unicode, true, true));
      assertEquals(ascii, URLUtil.convertIDNA2003(unicode, true, false));
      if (roundTrip) {
        assertEquals(unicode, URLUtil.convertIDNA2003(ascii, false, true));
        assertEquals(unicode, URLUtil.convertIDNA2003(ascii, false, false));
      }
    }
  }

  @Test
  public final void testConvertHostInvalid() {
    // broken Punycode
    assertDoesNotThrow(() -> assertEquals("xn--xn--bss-7z6ccid.com",
        URLUtil.convertIDNA2003("xn--xn--bss-7z6ccid.com", false, true)));

    // invalid Punycode
    assertThrows(MalformedURLException.class,
        () -> URLUtil.convertIDNA2008("xn--0.pt", false));

    // IDNA2003 not allowing characters not in Unicode 3.2
    assertThrows(MalformedURLException.class,
        () -> URLUtil.convertIDNA2003("☕.example", true, true));
    assertDoesNotThrow(() -> assertEquals("xn--53h.example",
        URLUtil.convertIDNA2003("xn--53h.example", false, true)));

    // IDNA2008 invalid,
    // cf. https://www.unicode.org/reports/tr46/#Implementation_Notes
    // cf. https://www.unicode.org/Public/17.0.0/idna/IdnaTestV2.txt
    // disallowed character: ⒈ (U+2488 - DIGIT ONE FULL STOP)
    assertThrows(MalformedURLException.class,
        () -> URLUtil.convertIDNA2008("\u2488com", true));
    assertThrows(MalformedURLException.class,
        () -> URLUtil.convertIDNA2008("xn--acom-0w1b", false));
    assertThrows(MalformedURLException.class,
        () -> URLUtil.convertIDNA2008("xn--xn--a--gua.pt", false));
    assertThrows(MalformedURLException.class,
        () -> URLUtil.convertIDNA2008("xn--a-ä.pt", false));
    assertThrows(MalformedURLException.class,
        () -> URLUtil.convertIDNA2008("xn--a-ä.pt", true));
  }

  @Test
  public void testConvertHostInvalidFromJuneCrawl() throws MalformedURLException {
      // Goodies from the June Crawl 2026
      URL u = new URL(
              "http://techsauce%20global%20summit%202023%20%E0%B9%80%E0%B8%9B%E0%B9%87%E0%B8%99%E0%B8%87%E0%B8%B2%E0%B8%99%E0%B8%9B%E0%B8%A3%E0%B8%B0%E0%B8%8A%E0%B8%B8%E0%B8%A1%E0%B8%A3%E0%B8%B0%E0%B8%94%E0%B8%B1%E0%B8%9A%E0%B9%82%E0%B8%A5%E0%B8%81%E0%B8%97%E0%B8%B5%E0%B9%88%E0%B8%88%E0%B8%B1%E0%B8%94%E0%B8%82%E0%B8%B6%E0%B9%89%E0%B8%99%E0%B9%83%E0%B8%99%E0%B8%A7%E0%B8%B1%E0%B8%99%E0%B8%97%E0%B8%B5%E0%B9%88%2016%E2%80%9317%20%E0%B8%AA%E0%B8%B4%E0%B8%87%E0%B8%AB%E0%B8%B2%E0%B8%84%E0%B8%A1%202566%20%E0%B8%93%20%E0%B8%A8%E0%B8%B9%E0%B8%99%E0%B8%A2%E0%B9%8C%E0%B8%9B%E0%B8%A3%E0%B8%B0%E0%B8%8A%E0%B8%B8%E0%B8%A1%E0%B9%81%E0%B8%AB%E0%B9%88%E0%B8%87%E0%B8%8A%E0%B8%B2%E0%B8%95%E0%B8%B4%E0%B8%AA%E0%B8%B4%E0%B8%A3%E0%B8%B4%E0%B8%81%E0%B8%B4%E0%B8%95%E0%B8%B4%E0%B9%8C%20%E0%B8%81%E0%B8%A3%E0%B8%B8%E0%B8%87%E0%B9%80%E0%B8%97%E0%B8%9E%E0%B8%AF%20%E0%B8%87%E0%B8%B2%E0%B8%99%E0%B8%99%E0%B8%B5%E0%B9%89%E0%B8%94%E0%B8%B6%E0%B8%87%E0%B8%94%E0%B8%B9%E0%B8%94%E0%B8%9C%E0%B8%B9%E0%B9%89%E0%B9%80%E0%B8%82%E0%B9%89%E0%B8%B2%E0%B8%A3%E0%B9%88%E0%B8%A7%E0%B8%A1%E0%B8%87%E0%B8%B2%E0%B8%99%E0%B8%88%E0%B8%B2%E0%B8%81%E0%B8%81%E0%B8%A7%E0%B9%88%E0%B8%B2%2050%20%E0%B8%9B%E0%B8%A3%E0%B8%B0%E0%B9%80%E0%B8%97%E0%B8%A8%20%E0%B9%82%E0%B8%94%E0%B8%A2%E0%B8%A1%E0%B8%B5%E0%B8%9C%E0%B8%B9%E0%B9%89%E0%B9%80%E0%B8%82%E0%B9%89%E0%B8%B2%E0%B8%A3%E0%B9%88%E0%B8%A7%E0%B8%A1%E0%B8%87%E0%B8%B2%E0%B8%99%E0%B8%A1%E0%B8%B2%E0%B8%81%E0%B8%81%E0%B8%A7%E0%B9%88%E0%B8%B2%2016,000%20%E0%B8%84%E0%B8%99%20%E0%B9%81%E0%B8%A5%E0%B8%B0%E0%B8%A1%E0%B8%B5%E0%B8%9C%E0%B8%B9%E0%B9%89%E0%B9%80%E0%B8%82%E0%B9%89%E0%B8%B2%E0%B8%A3%E0%B9%88%E0%B8%A7%E0%B8%A1%E0%B8%88%E0%B8%B2%E0%B8%81%E0%B8%99%E0%B8%B2%E0%B8%99%E0%B8%B2%E0%B8%9B%E0%B8%A3%E0%B8%B0%E0%B9%80%E0%B8%97%E0%B8%A8%E0%B8%A1%E0%B8%B2%E0%B8%81%E0%B8%96%E0%B8%B6%E0%B8%87%2040%25%20%20%E0%B9%83%E0%B8%99%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%88%E0%B8%B1%E0%B8%94%E0%B8%87%E0%B8%B2%E0%B8%99%E0%B8%84%E0%B8%A3%E0%B8%B1%E0%B9%89%E0%B8%87%E0%B8%99%E0%B8%B5%E0%B9%89%20%E0%B8%A1%E0%B8%B5%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%99%E0%B8%B3%E0%B9%80%E0%B8%AA%E0%B8%99%E0%B8%AD%E0%B8%88%E0%B8%B2%E0%B8%81%E0%B8%9C%E0%B8%B9%E0%B9%89%E0%B8%9A%E0%B8%A3%E0%B8%A3%E0%B8%A2%E0%B8%B2%E0%B8%A2%E0%B8%8A%E0%B8%B1%E0%B9%89%E0%B8%99%E0%B8%99%E0%B8%B3%20%E0%B8%A3%E0%B8%A7%E0%B8%A1%E0%B8%96%E0%B8%B6%E0%B8%87%E0%B9%82%E0%B8%8B%E0%B8%99%20business%20matching%20%E0%B8%97%E0%B8%B5%E0%B9%88%E0%B9%80%E0%B8%95%E0%B9%87%E0%B8%A1%E0%B9%84%E0%B8%9B%E0%B8%94%E0%B9%89%E0%B8%A7%E0%B8%A2%E0%B9%82%E0%B8%AD%E0%B8%81%E0%B8%B2%E0%B8%AA%E0%B8%AA%E0%B8%B3%E0%B8%AB%E0%B8%A3%E0%B8%B1%E0%B8%9A%E0%B8%9C%E0%B8%B9%E0%B9%89%E0%B8%9B%E0%B8%A3%E0%B8%B0%E0%B8%81%E0%B8%AD%E0%B8%9A%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%88%E0%B8%B2%E0%B8%81%E0%B8%AD%E0%B8%87%E0%B8%84%E0%B9%8C%E0%B8%81%E0%B8%A3%E0%B8%8A%E0%B8%B1%E0%B9%89%E0%B8%99%E0%B8%99%E0%B8%B3%20%E0%B8%AA%E0%B8%95%E0%B8%B2%E0%B8%A3%E0%B9%8C%E0%B8%97%E0%B8%AD%E0%B8%B1%E0%B8%9E%20smes%20%E0%B8%9C%E0%B8%B9%E0%B9%89%E0%B9%80%E0%B8%8A%E0%B8%B5%E0%B9%88%E0%B8%A2%E0%B8%A7%E0%B8%8A%E0%B8%B2%E0%B8%8D%E0%B8%94%E0%B9%89%E0%B8%B2%E0%B8%99%E0%B9%80%E0%B8%97%E0%B8%84%E0%B9%82%E0%B8%99%E0%B9%82%E0%B8%A5%E0%B8%A2%E0%B8%B5%20%E0%B9%81%E0%B8%A5%E0%B8%B0%E0%B8%9A%E0%B8%B8%E0%B8%84%E0%B8%84%E0%B8%A5%E0%B8%97%E0%B8%B1%E0%B9%88%E0%B8%A7%E0%B9%84%E0%B8%9B%20%E0%B9%80%E0%B8%9E%E0%B8%B7%E0%B9%88%E0%B8%AD%E0%B9%81%E0%B8%A5%E0%B8%81%E0%B9%80%E0%B8%9B%E0%B8%A5%E0%B8%B5%E0%B9%88%E0%B8%A2%E0%B8%99%E0%B8%84%E0%B8%A7%E0%B8%B2%E0%B8%A1%E0%B8%84%E0%B8%B4%E0%B8%94%E0%B9%80%E0%B8%AB%E0%B9%87%E0%B8%99%E0%B8%97%E0%B8%B2%E0%B8%87%E0%B8%98%E0%B8%B8%E0%B8%A3%E0%B8%81%E0%B8%B4%E0%B8%88%20%E0%B9%82%E0%B8%94%E0%B8%A2%E0%B8%97%E0%B8%B1%E0%B9%89%E0%B8%87%E0%B8%AB%E0%B8%A1%E0%B8%94%E0%B8%99%E0%B8%B5%E0%B9%89%E0%B9%84%E0%B8%94%E0%B9%89%E0%B8%AA%E0%B8%A3%E0%B9%89%E0%B8%B2%E0%B8%87%E0%B9%82%E0%B8%AD%E0%B8%81%E0%B8%B2%E0%B8%AA%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%97%E0%B8%B3%E0%B8%98%E0%B8%B8%E0%B8%A3%E0%B8%81%E0%B8%B4%E0%B8%88%E0%B9%80%E0%B8%9E%E0%B8%B4%E0%B9%88%E0%B8%A1%E0%B8%82%E0%B8%B6%E0%B9%89%E0%B8%99%E0%B8%96%E0%B8%B6%E0%B8%87%201,000%20%E0%B8%84%E0%B8%A3%E0%B8%B1%E0%B9%89%E0%B8%87%20%20%E0%B8%AA%E0%B8%A3%E0%B8%B8%E0%B8%9B%20%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B9%83%E0%B8%8A%E0%B9%89%20nft%20gen2%20%E0%B8%8B%E0%B8%B6%E0%B9%88%E0%B8%87%E0%B9%80%E0%B8%9B%E0%B9%87%E0%B8%99%E0%B8%AA%E0%B9%88%E0%B8%A7%E0%B8%99%E0%B8%AB%E0%B8%99%E0%B8%B6%E0%B9%88%E0%B8%87%E0%B8%82%E0%B8%AD%E0%B8%87%E0%B9%80%E0%B8%97%E0%B8%84%E0%B9%82%E0%B8%99%E0%B9%82%E0%B8%A5%E0%B8%A2%E0%B8%B5%20dynamic%20data%20layer%20%E0%B8%82%E0%B8%AD%E0%B8%87%E0%B8%97%E0%B8%B2%E0%B8%87%20six%20network%20%E0%B8%97%E0%B8%B5%E0%B9%88%E0%B8%87%E0%B8%B2%E0%B8%99%20techsauce%20global%20summit%202023%20%E0%B9%84%E0%B8%94%E0%B9%89%E0%B9%80%E0%B8%9E%E0%B8%B4%E0%B9%88%E0%B8%A1%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%A1%E0%B8%B5%E0%B8%AA%E0%B9%88%E0%B8%A7%E0%B8%99%E0%B8%A3%E0%B9%88%E0%B8%A7%E0%B8%A1%E0%B8%82%E0%B8%AD%E0%B8%87%E0%B8%9C%E0%B8%B9%E0%B9%89%E0%B9%83%E0%B8%8A%E0%B9%89%20%E0%B8%94%E0%B9%89%E0%B8%A7%E0%B8%A2%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B9%80%E0%B8%8A%E0%B8%B7%E0%B9%88%E0%B8%AD%E0%B8%A1%E0%B8%95%E0%B9%88%E0%B8%AD%20nft%20%E0%B9%80%E0%B8%82%E0%B9%89%E0%B8%B2%E0%B8%81%E0%B8%B1%E0%B8%9A%E0%B8%9B%E0%B8%A3%E0%B8%B0%E0%B8%AA%E0%B8%9A%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%93%E0%B9%8C%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%88%E0%B8%B1%E0%B8%94%E0%B8%87%E0%B8%B2%E0%B8%99%20%E0%B9%80%E0%B8%9B%E0%B8%A5%E0%B8%B5%E0%B9%88%E0%B8%A2%E0%B8%99%E0%B8%9B%E0%B8%A3%E0%B8%B0%E0%B8%AA%E0%B8%9A%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%93%E0%B9%8C%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B9%80%E0%B8%82%E0%B9%89%E0%B8%B2%E0%B8%A3%E0%B9%88%E0%B8%A7%E0%B8%A1%E0%B8%87%E0%B8%B2%E0%B8%99%E0%B9%81%E0%B8%9A%E0%B8%9A%E0%B8%97%E0%B8%B1%E0%B9%88%E0%B8%A7%20%E0%B9%86%20%E0%B9%84%E0%B8%9B%E0%B9%83%E0%B8%AB%E0%B9%89%E0%B9%80%E0%B8%9B%E0%B9%87%E0%B8%99%20%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B9%80%E0%B8%82%E0%B9%89%E0%B8%B2%E0%B8%A3%E0%B9%88%E0%B8%A7%E0%B8%A1%E0%B9%83%E0%B8%99%E0%B9%80%E0%B8%8A%E0%B8%B4%E0%B8%87%E0%B8%A3%E0%B8%B8%E0%B8%81%E0%B8%97%E0%B8%B5%E0%B9%88%E0%B8%97%E0%B8%B3%E0%B9%83%E0%B8%AB%E0%B9%89%E0%B9%80%E0%B8%81%E0%B8%B4%E0%B8%94%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%A1%E0%B8%B5%E0%B8%AA%E0%B9%88%E0%B8%A7%E0%B8%99%E0%B8%A3%E0%B9%88%E0%B8%A7%E0%B8%A1%E0%B9%81%E0%B8%9A%E0%B8%9A%E0%B9%84%E0%B8%A3%E0%B9%89%E0%B8%A3%E0%B8%AD%E0%B8%A2%E0%B8%95%E0%B9%88%E0%B8%AD%E0%B8%81%E0%B8%B1%E0%B8%9A%E0%B8%81%E0%B8%B4%E0%B8%88%E0%B8%81%E0%B8%A3%E0%B8%A3%E0%B8%A1%20%E0%B9%81%E0%B8%A5%E0%B8%B0%E0%B8%9E%E0%B8%B7%E0%B9%89%E0%B8%99%E0%B8%97%E0%B8%B5%E0%B9%88%E0%B8%88%E0%B8%B1%E0%B8%94%E0%B8%87%E0%B8%B2%E0%B8%99%E0%B8%95%E0%B9%88%E0%B8%B2%E0%B8%87%E0%B9%86%20%E0%B8%A1%E0%B8%B2%E0%B8%81%E0%B8%82%E0%B8%B6%E0%B9%89%E0%B8%99%20%20%E0%B8%94%E0%B9%89%E0%B8%A7%E0%B8%A2%E0%B8%A7%E0%B8%B4%E0%B8%98%E0%B8%B5%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%99%E0%B8%B3%E0%B9%80%E0%B8%AA%E0%B8%99%E0%B8%AD%E0%B8%A5%E0%B8%B1%E0%B8%81%E0%B8%A9%E0%B8%93%E0%B8%B0%E0%B8%99%E0%B8%B5%E0%B9%89%20%E0%B9%81%E0%B8%AA%E0%B8%94%E0%B8%87%E0%B9%83%E0%B8%AB%E0%B9%89%E0%B9%80%E0%B8%AB%E0%B9%87%E0%B8%99%E0%B8%96%E0%B8%B6%E0%B8%87%E0%B8%A8%E0%B8%B1%E0%B8%81%E0%B8%A2%E0%B8%A0%E0%B8%B2%E0%B8%9E%E0%B8%82%E0%B8%AD%E0%B8%87%20nft%20%E0%B9%83%E0%B8%99%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B9%80%E0%B8%9E%E0%B8%B4%E0%B9%88%E0%B8%A1%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%A1%E0%B8%B5%E0%B8%AA%E0%B9%88%E0%B8%A7%E0%B8%99%E0%B8%A3%E0%B9%88%E0%B8%A7%E0%B8%A1%E0%B9%83%E0%B8%99%E0%B8%81%E0%B8%B4%E0%B8%88%E0%B8%81%E0%B8%A3%E0%B8%A3%E0%B8%A1%E0%B8%95%E0%B9%88%E0%B8%B2%E0%B8%87%E0%B9%86%20%E0%B8%99%E0%B8%AD%E0%B8%81%E0%B8%88%E0%B8%B2%E0%B8%81%E0%B8%99%E0%B8%B5%E0%B9%89%20six%20network%20%E0%B8%A2%E0%B8%B1%E0%B8%87%E0%B8%A1%E0%B8%B5%E0%B9%80%E0%B8%9B%E0%B9%89%E0%B8%B2%E0%B8%AB%E0%B8%A1%E0%B8%B2%E0%B8%A2%E0%B8%97%E0%B8%B5%E0%B9%88%E0%B8%88%E0%B8%B0%E0%B8%9E%E0%B8%B1%E0%B8%92%E0%B8%99%E0%B8%B2%E0%B9%80%E0%B8%97%E0%B8%84%E0%B9%82%E0%B8%99%E0%B9%82%E0%B8%A5%E0%B8%A2%E0%B8%B5%20dynamic%20data%20layer%20%E0%B8%AD%E0%B8%A2%E0%B9%88%E0%B8%B2%E0%B8%87%E0%B8%95%E0%B9%88%E0%B8%AD%E0%B9%80%E0%B8%99%E0%B8%B7%E0%B9%88%E0%B8%AD%E0%B8%87%20%E0%B9%80%E0%B8%9E%E0%B8%B7%E0%B9%88%E0%B8%AD%E0%B9%80%E0%B8%9E%E0%B8%B4%E0%B9%88%E0%B8%A1%E0%B8%AD%E0%B8%B1%E0%B8%95%E0%B8%A3%E0%B8%B2%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B9%80%E0%B8%82%E0%B9%89%E0%B8%B2%E0%B8%96%E0%B8%B6%E0%B8%87%20%E0%B9%81%E0%B8%A5%E0%B8%B0%E0%B9%80%E0%B8%9E%E0%B8%B4%E0%B9%88%E0%B8%A1%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B8%A1%E0%B8%B5%E0%B8%AA%E0%B9%88%E0%B8%A7%E0%B8%99%E0%B8%A3%E0%B9%88%E0%B8%A7%E0%B8%A1%E0%B8%AA%E0%B8%B3%E0%B8%AB%E0%B8%A3%E0%B8%B1%E0%B8%9A%E0%B8%90%E0%B8%B2%E0%B8%99%E0%B8%9C%E0%B8%B9%E0%B9%89%E0%B9%83%E0%B8%8A%E0%B9%89%E0%B8%82%E0%B8%99%E0%B8%B2%E0%B8%94%E0%B9%83%E0%B8%AB%E0%B8%8D%E0%B9%88%E0%B9%81%E0%B8%A5%E0%B8%B0%E0%B8%AB%E0%B8%A5%E0%B8%B2%E0%B8%81%E0%B8%AB%E0%B8%A5%E0%B8%B2%E0%B8%A2%E0%B8%A1%E0%B8%B2%E0%B8%81%E0%B8%82%E0%B8%B6%E0%B9%89%E0%B8%99%20%20%E0%B8%95%E0%B8%B4%E0%B8%94%E0%B8%95%E0%B8%B2%E0%B8%A1%20six%20network%20%E0%B9%84%E0%B8%94%E0%B9%89%E0%B8%97%E0%B8%B5%E0%B9%88%20%20website%20l%20telegram%20l%20twitter%20l%20facebook%20l%20discord%20l%20medium/");
      String host = u.getHost();
      String hostLowerCase = host.toLowerCase(Locale.ROOT);
      assertThrows(MalformedURLException.class,
        () -> URLUtil.convertIDNA2008(hostLowerCase, true));
  }

  @Test
  public void testFileProtocol() throws Exception {
    // keep one single slash NUTCH-1483
    assertEquals("file:/path/file.html",
        URLUtil.toASCII("file:/path/file.html"));
    assertEquals("file:/path/file.html",
        URLUtil.toUNICODE("file:/path/file.html"));
  }

}
