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
package org.apache.nutch.fetcher;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.nutch.crawl.DbUpdaterJob;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.InjectorJob;
import org.apache.nutch.crawl.URLWebPage;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.AbstractNutchTest;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.CrawlTestUtil;
import org.mortbay.jetty.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import crawlercommons.robots.BaseRobotRules;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Various fetcher tests which test fetching, refetching, sitemap fetching
 * sitemap detection and the basic verification of a agent name check. 
 */
public class TestFetcher extends AbstractNutchTest {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractNutchTest.class);

  final static Path testdir = new Path("build/test/fetch-test");
  Path urlPath;
  Server server;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    conf.setBoolean(FetcherJob.PARSE_KEY, true);
    urlPath = new Path(testdir, "urls");
    server = CrawlTestUtil.getServer(conf.getInt("content.server.port", 50000),
        "build/test/data/fetch-test-site");
    server.start();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    server.stop();
    fs.delete(testdir, true);
    super.tearDown();
  }

  /**
   * Test only the normal web page Fetcher
   *
   * @throws Exception
   */
  @Test
  public void testFetch() throws Exception {

    String batchId = "1234";
    conf.set(GeneratorJob.BATCH_ID, batchId);

    // generate seedlist
    ArrayList<String> normalUrls = new ArrayList<String>();
    ArrayList<String> sitemapUrls = new ArrayList<String>();
    ArrayList<String> urls = new ArrayList<String>();

    addUrl(normalUrls, "index.html");
    addUrl(normalUrls, "pagea.html");
    addUrl(normalUrls, "pageb.html");
    addUrl(normalUrls, "dup_of_pagea.html");
    addUrl(normalUrls, "nested_spider_trap.html");
    addUrl(normalUrls, "exception.html");
    addUrl(sitemapUrls, "sitemap1.xml\t-sitemap");
    addUrl(sitemapUrls, "sitemap2.xml\t-sitemap");
    addUrl(sitemapUrls, "sitemapIndex.xml\t-sitemap");

    urls.addAll(normalUrls);
    urls.addAll(sitemapUrls);

    CrawlTestUtil.generateSeedList(fs, urlPath, urls);

    // inject
    InjectorJob injector = new InjectorJob(conf);
    injector.inject(urlPath);

    // generate
    long time = System.currentTimeMillis();
    GeneratorJob g = new GeneratorJob(conf);
    //  generate for non sitemap
    g.generate(Long.MAX_VALUE, time, false, false, false);
    //    generate for only sitemap
    g.generate(Long.MAX_VALUE, time, false, false, true);

    // fetch
    time = System.currentTimeMillis();
    FetcherJob fetcher = new FetcherJob(conf);
    fetcher.fetch(batchId, 1, false, -1);

    time = System.currentTimeMillis() - time;

    // verify politeness, time taken should be more than (num_of_pages +1)*delay
    int minimumTime = (int) ((urls.size() + 1) * 1000 * conf.getFloat(
        "fetcher.server.delay", 5));
    assertTrue(time > minimumTime);

    List<URLWebPage> pages = CrawlTestUtil.readContents(webPageStore,
        Mark.FETCH_MARK, (String[]) null);
    assertEquals(normalUrls.size(), pages.size());
    List<String> handledurls = new ArrayList<String>();
    for (URLWebPage up : pages) {
      ByteBuffer bb = up.getDatum().getContent();
      if (bb == null) {
        continue;
      }
      String content = Bytes.toString(bb);
      if (content.indexOf("Nutch fetcher test page") != -1) {
        handledurls.add(up.getUrl());
      }
    }
    Collections.sort(normalUrls);
    Collections.sort(handledurls);

    // verify that enough pages were handled
    assertEquals(normalUrls.size(), handledurls.size());

    // verify that correct pages were handled
    assertTrue(handledurls.containsAll(normalUrls));
    assertTrue(normalUrls.containsAll(handledurls));
  }

  /**
   * Tests a refetch of a URL. This process consists of two consecutive
   * inject, generate, fetch, parse then update cycles. The test configuration
   * is defined such that <code>db.fetch.interval.default</code> is set to
   * a very low value (indicating that the URL should be fetched again immediately).
   * In addition, configuration tests that relevant 
   * {@link org.apache.nutch.metadata.Metadata} is present and the values consistent 
   * and therefore not overwritten.
   *
   * @throws Exception
   *
   * @see <a href="https://issues.apache.org/jira/browse/NUTCH-2222">https://issues.apache.org/jira/browse/NUTCH-2222</a>
   */
  @Test
  public void testReFetch() throws Exception {

    // generate seedlist
    ArrayList<String> urls = new ArrayList<String>();
    // inject
    addUrl(urls, "index.html");
    CrawlTestUtil.generateSeedList(fs, urlPath, urls);

    InjectorJob injector = new InjectorJob(conf);
    injector.inject(urlPath);

    // crawl 1 
    long time = System.currentTimeMillis();
    GeneratorJob g = new GeneratorJob(conf);
    String batchId = g.generate(Long.MAX_VALUE, time, false, false, false);
    FetcherJob fetcher = new FetcherJob(conf);
    fetcher.fetch(Nutch.ALL_BATCH_ID_STR, 1, false, -1);
    ParserJob parser = new ParserJob(conf);
    parser.parse(Nutch.ALL_BATCH_ID_STR, true, true);
    URLWebPage up = CrawlTestUtil.readContents(webPageStore, Mark.FETCH_MARK, (String[]) null).get(0);
    assertEquals(urls.size(), 1);
    int countMetaDatasFetch1 = up.getDatum().getMetadata().size();
    DbUpdaterJob updateter = new DbUpdaterJob(conf);
    updateter.run(new String[]{Nutch.ALL_BATCH_ID_STR});


    Thread.sleep(10000);

    // crawl 2
    CrawlTestUtil.generateSeedList(fs, urlPath, urls);
    injector = new InjectorJob(conf);
    injector.inject(urlPath);
    g = new GeneratorJob(conf);
    time = System.currentTimeMillis();
    batchId = g.generate(Long.MAX_VALUE, time, false, false, false); 
    fetcher = new FetcherJob(conf);
    fetcher.fetch(Nutch.ALL_BATCH_ID_STR, 1, false, -1);
    parser = new ParserJob(conf);
    parser.parse(Nutch.ALL_BATCH_ID_STR, true, true);
    updateter = new DbUpdaterJob(conf);
    updateter.run(new String[]{Nutch.ALL_BATCH_ID_STR});
    up = CrawlTestUtil.readContents(webPageStore, null, (String[]) null).get(0);
    assertEquals(urls.size(), 1);
    int countMetaDatasFetch2 = up.getDatum().getMetadata().size();

    LOG.info("countMetaDatas Fetch1 : {}",  countMetaDatasFetch1);
    LOG.info("countMetaDatas Fetch2 : {}",  countMetaDatasFetch2);
    assertEquals(countMetaDatasFetch1, countMetaDatasFetch2);
  }

  /**
   * Test that only sitemap page fetcher
   *
   * @throws Exception
   */
  @Test
  public void testSitemapFetch() throws Exception {
    String batchId = "1234";
    conf.set(GeneratorJob.BATCH_ID, batchId);

    // generate seedlist
    ArrayList<String> normalUrls = new ArrayList<String>();
    ArrayList<String> sitemapUrls = new ArrayList<String>();
    ArrayList<String> urls = new ArrayList<String>();

    addUrl(normalUrls, "index.html");
    addUrl(normalUrls, "pagea.html");
    addUrl(normalUrls, "pageb.html");
    addUrl(normalUrls, "dup_of_pagea.html");
    addUrl(normalUrls, "nested_spider_trap.html");
    addUrl(normalUrls, "exception.html");
    addUrl(sitemapUrls, "sitemap1.xml\t-sitemap");
    addUrl(sitemapUrls, "sitemap2.xml\t-sitemap");
    addUrl(sitemapUrls, "sitemapIndex.xml\t-sitemap");

    urls.addAll(normalUrls);
    urls.addAll(sitemapUrls);

    String[] fields = new String[] {
        WebPage.Field.MARKERS.getName(), WebPage.Field.SCORE.getName() };

    Path urlPath = new Path(testdir, "urls");

    CrawlTestUtil.generateSeedList(fs, urlPath, urls);

    InjectorJob injector = new InjectorJob();
    injector.setConf(conf);
    injector.inject(urlPath);

    // generate
    long time = System.currentTimeMillis();
    GeneratorJob g = new GeneratorJob(conf);

    //    generate for non sitemap
    g.generate(Long.MAX_VALUE, time, false, false, false);
    //    generate for only sitemap
    g.generate(Long.MAX_VALUE, time, false, false, true);

    FetcherJob fetcher = new FetcherJob(conf);

    // for only sitemap fetch
    fetcher.fetch(batchId, 1, false, -1, false, true);

    List<URLWebPage> pages = CrawlTestUtil.readContents(webPageStore,
        Mark.FETCH_MARK, (String[]) null);
    assertEquals(sitemapUrls.size(), pages.size());
    List<String> handledurls = new ArrayList<String>();
    for (URLWebPage up : pages) {
      ByteBuffer bb = up.getDatum().getContent();
      if (bb == null) {
        continue;
      }
      String content = Bytes.toString(bb);
      if (content.indexOf("sitemap") != -1) {
        handledurls.add(up.getUrl() + "\t-sitemap");
      }
    }
    Collections.sort(sitemapUrls);
    Collections.sort(handledurls);

    // verify that enough pages were handled
    assertEquals(sitemapUrls.size(), handledurls.size());

    // verify that correct pages were handled
    assertTrue(handledurls.containsAll(sitemapUrls));
    assertTrue(sitemapUrls.containsAll(handledurls));

  }

  /**
   * Test that sitemap detection from robot.txt
   *
   * @throws Exception
   */
  @Test
  public void testSitemapDetect() throws Exception {
    String batchId = "1234";
    conf.set(GeneratorJob.BATCH_ID, batchId);

    // generate seedlist
    ArrayList<String> urls = new ArrayList<String>();

    addUrl(urls, "");

    String[] fields = new String[] {
        WebPage.Field.MARKERS.getName(), WebPage.Field.SCORE.getName() };

    Path urlPath = new Path(testdir, "urls");

    CrawlTestUtil.generateSeedList(fs, urlPath, urls);

    InjectorJob injector = new InjectorJob();
    injector.setConf(conf);
    injector.inject(urlPath);

    // generate
    long time = System.currentTimeMillis();
    GeneratorJob g = new GeneratorJob(conf);

    g.generate(Long.MAX_VALUE, time, false, false, false);

    FetcherJob fetcher = new FetcherJob(conf);

    // for only sitemap fetch
    fetcher.fetch(batchId, 1, false, -1, true, false);

    List<URLWebPage> pages = CrawlTestUtil.readContents(webPageStore,
        Mark.FETCH_MARK, (String[]) null);
    assertEquals(urls.size(), pages.size());
    for (URLWebPage up : pages) {

      ProtocolFactory protocolFactory = new ProtocolFactory(conf);
      Protocol protocol = protocolFactory.getProtocol(up.getUrl());
      BaseRobotRules rules = protocol.getRobotRules(up.getUrl(),
          up.getDatum());

      Map<CharSequence, CharSequence> sitemaps = up.getDatum().getSitemaps();
      assertEquals(rules.getSitemaps().size(),
          sitemaps.size()); // robots.txt file has 3 sitemap urls.
    }
  }

  /** 
   * Maps a webpage to the local Jetty server address so that it can 
   * be fetched as part of an arraylist
   */
  private void addUrl(ArrayList<String> urls, String page) {
    urls.add("http://127.0.0.1:" + server.getConnectors()[0].getPort() + "/"
        + page);
  }

  @Test
  public void testAgentNameCheck() {

    boolean failedNoAgentName = false;
    conf.set("http.agent.name", "");

    try {
      FetcherJob fetcher = new FetcherJob(conf);
      fetcher.checkConfiguration();
    } catch (IllegalArgumentException iae) {
      String message = iae.getMessage();
      failedNoAgentName = message.equals("Fetcher: No agents listed in "
          + "'http.agent.name' property.");
    } catch (Exception e) {
    }

    assertTrue(failedNoAgentName);
  }

}
