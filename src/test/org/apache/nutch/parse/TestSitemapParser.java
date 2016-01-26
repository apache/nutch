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

package org.apache.nutch.parse;

import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.nutch.crawl.InjectType;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.AbstractNutchTest;
import org.apache.nutch.util.TableUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSitemapParser extends AbstractNutchTest {

  @Mock
  WebPage page;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    page =mock(WebPage.class);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testSitemapParser() throws Exception {
    String sitemapUrl = "http://localhost/sitemap.xml";
    int urlSize = 5;
    String content = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        + "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.sitemaps.org/schemas/sitemap/0.9 http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd\">\n\t"
        + "<url>\n\t\t<loc>http://localhost/zzz1.html</loc>\n\t\t<lastmod>2015-06-10</lastmod>\n\t\t<changefreq>monthly</changefreq>tml\n\t\t<priority>0.8</priority>\n\t</url>\n\t"
        + "<url>\n\t\t<loc>http://localhost/zzz2.html</loc>\n\t\t<lastmod>2015-06-10</lastmod>\n\t\t<changefreq>monthly</changefreq>\n\t\t<priority>0.8</priority>\n\t</url>\n\t"
        + "<url>\n\t\t<loc>http://localhost/zzz3.html</loc>\n\t\t<lastmod>2015-06-10</lastmod>\n\t\t<changefreq>monthly</changefreq>\n\t\t<priority>0.8</priority>\n\t</url>\n\t"
        + "<url>\n\t\t<loc>http://localhost/zzz4.html</loc>\n\t\t<lastmod>2015-06-10</lastmod>\n\t\t<changefreq>monthly</changefreq>\n\t\t<priority>0.8</priority>\n\t</url>\n\t"
        + "<url>\n\t\t<loc>http://localhost/zzz5.html</loc>\n\t\t<lastmod>2015-06-10</lastmod>\n\t\t<changefreq>monthly</changefreq>\n\t\t<priority>0.8</priority>\n\t</url>\n"
        + "</urlset>";
    when(page.getContent()).thenReturn(ByteBuffer.wrap(content.getBytes()));
    when(page.getContentType()).thenReturn("application/xml");

    NutchSitemapParser sParser = new NutchSitemapParser();
    NutchSitemapParse nutchSitemapParse = sParser.getParse(sitemapUrl, page);

    assertNotNull(nutchSitemapParse);

    ParseStatus pstatus = nutchSitemapParse.getParseStatus();
    assertTrue(ParseStatusUtils.isSuccess(pstatus));

    assertEquals(nutchSitemapParse.getOutlinkMap().size(), urlSize);
  }

  @Test
  public void testSitemapIndexParser() throws Exception {
    String sitemapUrl = "http://localhost/sitemapIndex.xml";
    int urlSize = 3;
    String content = "<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n"
        + "    <sitemap>\n"
        + "        <loc>http://localhost/sitemap1.xml</loc>\n"
        + "        <lastmod>2015-07-30</lastmod>\n"
        + "    </sitemap>\n"
        + "    <sitemap>\n"
        + "        <loc>http://localhost/sitemap2.xml</loc>\n"
        + "        <lastmod>2015-07-30</lastmod>\n"
        + "    </sitemap>\n"
        + "    <sitemap>\n"
        + "        <loc>http://localhost/sitemap3.xml</loc>\n"
        + "        <lastmod>2015-07-30</lastmod>\n"
        + "    </sitemap>\n"
        + "</sitemapindex>";

    when(page.getContent()).thenReturn(ByteBuffer.wrap(content.getBytes()));
    when(page.getContentType()).thenReturn("application/xml");
    when(page.getSitemaps()).thenReturn(new HashMap<CharSequence, CharSequence>());

    NutchSitemapParser sParser = new NutchSitemapParser();
    NutchSitemapParse nutchSitemapParse = sParser.getParse(sitemapUrl, page);

    assertNotNull(nutchSitemapParse);
    assertNull(nutchSitemapParse.getOutlinkMap());

    ParseStatus pstatus = nutchSitemapParse.getParseStatus();
    assertTrue(ParseStatusUtils.isSuccess(pstatus));

    assertEquals(page.getSitemaps().size(), urlSize);
  }
}
