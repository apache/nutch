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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.AbstractNutchTest;
import org.apache.nutch.util.CrawlTestUtil;
import org.apache.nutch.util.TableUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;

/**
 * Basic generator test. 
 * <ol>
 * <li>Insert entries in webtable</li>
 * <li>Generates entries to fetch</li>
 * <li>Verifies that number of generated urls match, and finally </li>
 * <li>Verifies that highest scoring urls are generated.</li>
 * </ol>
 * 
 */
public class TestGenerator extends AbstractNutchTest {

  private static String[] FIELDS = new String[] {
      WebPage.Field.MARKERS.getName(), WebPage.Field.SCORE.getName() };

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Test that generator generates fetchlist ordered by score (desc).
   * 
   * @throws Exception
   */
  @Test
  @Ignore("GORA-240 Tests for MemStore")
  public void testGenerateHighest() throws Exception {

    String batchId = "1234";
    conf.set(GeneratorJob.BATCH_ID, batchId);

    final int NUM_RESULTS = 2;

    ArrayList<URLWebPage> list = new ArrayList<URLWebPage>();

    for (int i = 0; i <= 100; i++) {
      list.add(createURLWebPage("http://aaa/" + pad(i), 1, i));
    }

    for (URLWebPage uwp : list) {
      webPageStore.put(TableUtil.reverseUrl(uwp.getUrl()), uwp.getDatum());
    }
    CrawlTestUtil.generateFetchlist(NUM_RESULTS, conf, false, false);

    ArrayList<URLWebPage> l = CrawlTestUtil.readContents(webPageStore,
        Mark.GENERATE_MARK, FIELDS);

    // sort urls by score desc
    Collections.sort(l, new ScoreComparator());

    // verify we got right amount of records
    assertEquals(NUM_RESULTS, l.size());

    // verify we have the highest scoring urls
    assertEquals("http://aaa/100", (l.get(0).getUrl().toString()));
    assertEquals("http://aaa/099", (l.get(1).getUrl().toString()));
  }

  private String pad(int i) {
    String s = Integer.toString(i);
    while (s.length() < 3) {
      s = "0" + s;
    }
    return s;
  }

  /**
   * Comparator that sorts by score desc.
   */
  public class ScoreComparator implements Comparator<URLWebPage> {

    public int compare(URLWebPage tuple1, URLWebPage tuple2) {
      if (tuple2.getDatum().getScore() - tuple1.getDatum().getScore() < 0) {
        return -1;
      }
      if (tuple2.getDatum().getScore() - tuple1.getDatum().getScore() > 0) {
        return 1;
      }
      return 0;
    }
  }

  /**
   * Test that generator obeys the property "generate.max.count" and
   * "generate.count.mode".
   * 
   * @throws Exception
   */
  @Test
  @Ignore("GORA-240 Tests for MemStore")
  public void testGenerateHostLimit() throws Exception {

    String batchId = "1234";
    conf.set(GeneratorJob.BATCH_ID, batchId);

    ArrayList<URLWebPage> list = new ArrayList<URLWebPage>();

    list.add(createURLWebPage("http://www.example.com/index1.html", 1, 1));
    list.add(createURLWebPage("http://www.example.com/index2.html", 1, 1));
    list.add(createURLWebPage("http://www.example.com/index3.html", 1, 1));

    for (URLWebPage uwp : list) {
      webPageStore.put(TableUtil.reverseUrl(uwp.getUrl()), uwp.getDatum());
    }

    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.setInt(GeneratorJob.GENERATOR_MAX_COUNT, 1);
    myConfiguration.set(GeneratorJob.GENERATOR_COUNT_MODE,
        GeneratorJob.GENERATOR_COUNT_VALUE_HOST);
    CrawlTestUtil.generateFetchlist(Integer.MAX_VALUE, myConfiguration, false,
        false);

    ArrayList<URLWebPage> fetchList = CrawlTestUtil.readContents(webPageStore,
        Mark.GENERATE_MARK, FIELDS);

    // verify we got right amount of records
    assertEquals(1, fetchList.size());

    myConfiguration = new Configuration(conf);
    myConfiguration.setInt(GeneratorJob.GENERATOR_MAX_COUNT, 2);
    CrawlTestUtil.generateFetchlist(Integer.MAX_VALUE, myConfiguration, false,
        false);
    fetchList = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK,
        FIELDS);

    // verify we got right amount of records
    assertEquals(3, fetchList.size()); // 3 as 2 + 1 skipped (already generated)

    myConfiguration = new Configuration(conf);
    myConfiguration.setInt(GeneratorJob.GENERATOR_MAX_COUNT, 3);
    CrawlTestUtil.generateFetchlist(Integer.MAX_VALUE, myConfiguration, false,
        false);
    fetchList = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK,
        FIELDS);

    // verify we got right amount of records
    assertEquals(3, fetchList.size()); // 3 as now all have generate mark
  }

  /**
   * Test that generator obeys the property "generator.max.count" and
   * "generator.count.value=domain".
   * 
   * @throws Exception
   */
  @Test
  @Ignore("GORA-240 Tests for MemStore")
  public void testGenerateDomainLimit() throws Exception {
    String batchId = "1234";
    conf.set(GeneratorJob.BATCH_ID, batchId);
    ArrayList<URLWebPage> list = new ArrayList<URLWebPage>();

    list.add(createURLWebPage("http://one.example.com/index.html", 1, 1));
    list.add(createURLWebPage("http://one.example.com/index1.html", 1, 1));
    list.add(createURLWebPage("http://two.example.com/index.html", 1, 1));
    list.add(createURLWebPage("http://two.example.com/index1.html", 1, 1));
    list.add(createURLWebPage("http://three.example.com/index.html", 1, 1));
    list.add(createURLWebPage("http://three.example.com/index1.html", 1, 1));

    for (URLWebPage uwp : list) {
      webPageStore.put(TableUtil.reverseUrl(uwp.getUrl()), uwp.getDatum());
    }

    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.setInt(GeneratorJob.GENERATOR_MAX_COUNT, 1);
    myConfiguration.set(GeneratorJob.GENERATOR_COUNT_MODE,
        GeneratorJob.GENERATOR_COUNT_VALUE_DOMAIN);

    CrawlTestUtil.generateFetchlist(Integer.MAX_VALUE, myConfiguration, false,
        false);
    ArrayList<URLWebPage> fetchList = CrawlTestUtil.readContents(webPageStore,
        Mark.GENERATE_MARK, FIELDS);

    // verify we got right amount of records
    assertEquals(1, fetchList.size());

    myConfiguration = new Configuration(myConfiguration);
    myConfiguration.setInt(GeneratorJob.GENERATOR_MAX_COUNT, 2);
    CrawlTestUtil.generateFetchlist(Integer.MAX_VALUE, myConfiguration, false,
        false);
    fetchList = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK,
        FIELDS);

    // verify we got right amount of records
    assertEquals(3, fetchList.size()); // 2 + 1 skipped (already generated)

    myConfiguration = new Configuration(myConfiguration);
    myConfiguration.setInt(GeneratorJob.GENERATOR_MAX_COUNT, 3);
    CrawlTestUtil.generateFetchlist(Integer.MAX_VALUE, myConfiguration, false,
        false);
    fetchList = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK,
        FIELDS);

    // verify we got right amount of records
    assertEquals(6, fetchList.size()); // 3 + 3 skipped (already generated)
  }

  /**
   * Test generator obeys the filter setting.
   * 
   * @throws Exception
   * @throws IOException
   */
  @Test
  @Ignore("GORA-240 Tests for MemStore")
  public void testFilter() throws IOException, Exception {

    String batchId = "1234";
    conf.set(GeneratorJob.BATCH_ID, batchId);

    ArrayList<URLWebPage> list = new ArrayList<URLWebPage>();

    list.add(createURLWebPage("http://www.example.com/index.html", 1, 1));
    list.add(createURLWebPage("http://www.example.net/index.html", 1, 1));
    list.add(createURLWebPage("http://www.example.org/index.html", 1, 1));

    for (URLWebPage uwp : list) {
      webPageStore.put(TableUtil.reverseUrl(uwp.getUrl()), uwp.getDatum());
    }

    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.set("urlfilter.suffix.file", "filter-all.txt");

    CrawlTestUtil.generateFetchlist(Integer.MAX_VALUE, myConfiguration, true,
        false);
    ArrayList<URLWebPage> fetchList = CrawlTestUtil.readContents(webPageStore,
        Mark.GENERATE_MARK, FIELDS);

    assertEquals(0, fetchList.size());

    CrawlTestUtil.generateFetchlist(Integer.MAX_VALUE, myConfiguration, true,
        false);
    fetchList = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK,
        FIELDS);

    // verify nothing got filtered
    assertEquals(list.size(), fetchList.size());

  }

  @Test
  public void testGenerateOnlySitemap() throws Exception {
    boolean sitemap = true;
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

    ArrayList<URLWebPage> fetchList = generateForSitemap(urls, sitemap);

    assertEquals(2, fetchList.size());
  }
  
  /**
   * Test that generator generates fetchlist for only sitemaps.
   *
   * @throws Exception
   */
  @Test
  public void testGenerateNoneSitemap() throws Exception {
    boolean sitemap = false;
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

    ArrayList<URLWebPage> fetchList = generateForSitemap(urls, sitemap);

    assertEquals(10, fetchList.size());

  }

  private ArrayList<URLWebPage> generateForSitemap(ArrayList<String> urls,
      boolean sitemap) throws Exception {
    Path urlPath = new Path(testdir, "urls");
    String batchId = "1234";
    conf.set(GeneratorJob.BATCH_ID, batchId);

    CrawlTestUtil.generateSeedList(fs, urlPath, urls);

    InjectorJob injector = new InjectorJob();
    injector.setConf(conf);
    injector.inject(urlPath);

    Configuration myConfiguration = new Configuration(conf);
    CrawlTestUtil.generateFetchlist(Integer.MAX_VALUE, myConfiguration, true,
        sitemap);

    ArrayList<URLWebPage> fetchList = CrawlTestUtil.readContents(webPageStore,
        Mark.GENERATE_MARK, FIELDS);

    return fetchList;
  }

  /**
   * Constructs new {@link URLWebPage} from submitted parameters.
   * 
   * @param url
   *          url to use
   * @param fetchInterval
   * @param score
   * @return Constructed object
   */
  private URLWebPage createURLWebPage(final String url,
      final int fetchInterval, final float score) {
    WebPage page = WebPage.newBuilder().build();
    page.setFetchInterval(fetchInterval);
    page.setScore(score);
    page.setStatus((int) CrawlStatus.STATUS_UNFETCHED);
    return new URLWebPage(url, page);
  }

}
