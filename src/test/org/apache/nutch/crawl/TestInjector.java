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
package org.apache.nutch.crawl;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.AbstractNutchTest;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.CrawlTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Basic injector test: 1. Creates a text file with urls 2. Injects them into
 * crawldb 3. Reads crawldb entries and verifies contents 4. Injects more urls
 * into webdb 5. Reads crawldb entries and verifies contents
 * 
 */
public class TestInjector extends AbstractNutchTest {
  Path urlPath;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    urlPath = new Path(testdir, "urls");
  }

  /**
   * Test that injector for sitemap url
   *
   * @throws Exception
   */
  @Test
  public void testSitemapInject() throws Exception {
    ArrayList<String> urls = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      urls.add("http://zzz.com/" + i + ".html\tnutch.score=" + i
          + "\tcustom.attribute=" + i);
    }
    int sitemapUrlCnt = 2;
    for (int i = 10; i < 10 + sitemapUrlCnt; i++) {
      urls.add("http://zzz.com/" + i + ".html\tnutch.score=" + i
          + "\tcustom.attribute=" + i
          + "\t-sitemap");
    }

    CrawlTestUtil.generateSeedList(fs, urlPath, urls);

    InjectorJob injector = new InjectorJob();
    injector.setConf(conf);
    injector.inject(urlPath);

    List<URLWebPage> pages = CrawlTestUtil.readContents(webPageStore, null,
        fields);
    ArrayList<String> read = new ArrayList<String>();

    int sitemapCount = 0;

    for (URLWebPage up : pages) {
      WebPage page = up.getDatum();
      String representation = up.getUrl();
      representation += "\tnutch.score=" + page.getScore().intValue();
      ByteBuffer bb = page.getMetadata().get(new Utf8("custom.attribute"));
      if (bb != null) {
        representation += "\tcustom.attribute=" + Bytes.toString(bb);
      }
      if (URLFilters.isSitemap(page)) {
        representation += "\t-sitemap";
        sitemapCount++;
      }
      read.add(representation);
    }

    Collections.sort(read);
    Collections.sort(urls);

    assertEquals(urls.size(), read.size());

    assertTrue(urls.containsAll(read));
    assertTrue(read.containsAll(urls));

    assertEquals(sitemapCount, sitemapUrlCnt);

  }

  /**
   * Test that injector for multi sitemap urls
   *
   * @throws Exception
   */
  @Test
  public void testMultiSitemapInject() throws Exception {
    ArrayList<String> urls = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      urls.add("http://zzz" + i + ".com/\tnutch.score=" + i
          + "\tcustom.attribute=" + i);
    }

    int sitemapUrlCnt = 2;
    for (int i = 10; i < 10 + sitemapUrlCnt; i++) {
      String url = "http://zzz.com/" + i + ".html\tnutch.score=" + i
          + "\tcustom.attribute=" + i
          + "\tsitemaps:";

      for (int j = 0; j < sitemapUrlCnt; j++) {
        url += " sitemap" + j + ".xml";
      }

      urls.add(url);
    }

    CrawlTestUtil.generateSeedList(fs, urlPath, urls);

    InjectorJob injector = new InjectorJob();
    injector.setConf(conf);
    injector.inject(urlPath);

    List<URLWebPage> pages = CrawlTestUtil.readContents(webPageStore, null,
        fields);
    ArrayList<String> read = new ArrayList<String>();

    int sitemapCount = 0;

    for (URLWebPage up : pages) {
      WebPage page = up.getDatum();
      if (URLFilters.isSitemap(page)) {
        sitemapCount++;

      }
    }

    assertEquals(sitemapCount, sitemapUrlCnt * sitemapUrlCnt);
    assertEquals(urls.size() + sitemapUrlCnt * sitemapUrlCnt, pages.size());

  }

  /**
   * Test for Injector
   *
   * @throws Exception
   */
  @Test
  public void testInject() throws Exception {
    ArrayList<String> urls = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      urls.add("http://zzz.com/" + i + ".html\tnutch.score=" + i
          + "\tcustom.attribute=" + i);
    }
    CrawlTestUtil.generateSeedList(fs, urlPath, urls);

    InjectorJob injector = new InjectorJob();
    injector.setConf(conf);
    injector.inject(urlPath);

    // verify results
    List<String> read = readDb();

    Collections.sort(read);
    Collections.sort(urls);

    assertEquals(urls.size(), read.size());

    assertTrue(urls.containsAll(read));
    assertTrue(read.containsAll(urls));

    // inject more urls
    ArrayList<String> urls2 = new ArrayList<String>();
    ArrayList<String> urlsCheck = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      String u = "http://xxx.com/" + i + ".html";
      urls2.add(u);
      urlsCheck.add(u + "\tnutch.score=1");
    }
    CrawlTestUtil.generateSeedList(fs, urlPath, urls2);
    injector.inject(urlPath);
    urls.addAll(urlsCheck);

    // verify results
    read = readDb();

    Collections.sort(read);
    Collections.sort(urls);

    assertEquals(urls.size(), read.size());

    assertTrue(read.containsAll(urls));
    assertTrue(urls.containsAll(read));

  }

  private static final String[] fields = new String[] {
      WebPage.Field.MARKERS.getName(), WebPage.Field.METADATA.getName(),
      WebPage.Field.SCORE.getName() };

  private List<String> readDb() throws Exception {
    List<URLWebPage> pages = CrawlTestUtil.readContents(webPageStore, null,
        fields);
    ArrayList<String> read = new ArrayList<String>();
    for (URLWebPage up : pages) {
      WebPage page = up.getDatum();
      String representation = up.getUrl();
      representation += "\tnutch.score=" + page.getScore().intValue();
      ByteBuffer bb = page.getMetadata().get(new Utf8("custom.attribute"));
      if (bb != null) {
        representation += "\tcustom.attribute=" + Bytes.toString(bb);
      }
      read.add(representation);
    }
    return read;
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
}
