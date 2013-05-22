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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.AbstractNutchTest;
import org.apache.nutch.util.CrawlTestUtil;
import org.apache.nutch.util.TableUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Basic generator test. 1. Insert entries in webtable 2. Generates entries to
 * fetch 3. Verifies that number of generated urls match 4. Verifies that
 * highest scoring urls are generated
 *
 */
public class TestGenerator extends AbstractNutchTest {

  public static final Logger LOG = LoggerFactory.getLogger(TestGenerator.class);

  private static String[] FIELDS = new String[] {
    WebPage.Field.MARKERS.getName(),
    WebPage.Field.SCORE.getName()
  };
  
  @Override
  @Before
  public void setUp() throws Exception{
    super.setUp();
  }
  
  @Override
  @After
  public void tearDown()throws Exception {
    super.tearDown();
  }

  /**
   * Test that generator generates fetchlist ordered by score (desc).
   *
   * @throws Exception
   */
  @Test
  @Ignore("Temporarily diable until NUTCH-1572 is addressed.")
  public void testGenerateHighest() throws Exception {

    final int NUM_RESULTS = 2;

    ArrayList<URLWebPage> list = new ArrayList<URLWebPage>();

    for (int i = 0; i <= 100; i++) {
      list.add(createURLWebPage("http://aaa/" + pad(i), 1, i));
    }

    for (URLWebPage uwp : list) {
      webPageStore.put(TableUtil.reverseUrl(uwp.getUrl()), uwp.getDatum());
    }
    webPageStore.flush();

    generateFetchlist(NUM_RESULTS, conf, false);

    ArrayList<URLWebPage> l = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK, FIELDS);

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
   * Test that generator obeys the property "generate.max.count" and "generate.count.mode".
   *
   * @throws Exception
   */
  @Test
  @Ignore("Temporarily diable until NUTCH-1572 is addressed.")
  public void testGenerateHostLimit() throws Exception {
    ArrayList<URLWebPage> list = new ArrayList<URLWebPage>();

    list.add(createURLWebPage("http://www.example.com/index1.html", 1, 1));
    list.add(createURLWebPage("http://www.example.com/index2.html", 1, 1));
    list.add(createURLWebPage("http://www.example.com/index3.html", 1, 1));

    for (URLWebPage uwp : list) {
      webPageStore.put(TableUtil.reverseUrl(uwp.getUrl()), uwp.getDatum());
    }
    webPageStore.flush();

    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.setInt(GeneratorJob.GENERATOR_MAX_COUNT, 1);
    myConfiguration.set(GeneratorJob.GENERATOR_COUNT_MODE, GeneratorJob.GENERATOR_COUNT_VALUE_HOST);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    ArrayList<URLWebPage> fetchList = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK, FIELDS);

    // verify we got right amount of records
    assertEquals(1, fetchList.size());

    myConfiguration = new Configuration(conf);
    myConfiguration.setInt(GeneratorJob.GENERATOR_MAX_COUNT, 2);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK, FIELDS);

    // verify we got right amount of records
    assertEquals(3, fetchList.size()); //3 as 2 + 1 skipped (already generated)

    myConfiguration = new Configuration(conf);
    myConfiguration.setInt(GeneratorJob.GENERATOR_MAX_COUNT, 3);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK, FIELDS);

    // verify we got right amount of records
    assertEquals(3, fetchList.size()); //3 as now all have generate mark 
  }

  /**
   * Test that generator obeys the property "generator.max.count" and
   * "generator.count.value=domain".
   *
   * @throws Exception
   */
  @Test
  @Ignore("Temporarily diable until NUTCH-1572 is addressed.")
  public void testGenerateDomainLimit() throws Exception {
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
    webPageStore.flush();

    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.setInt(GeneratorJob.GENERATOR_MAX_COUNT, 1);
    myConfiguration.set(GeneratorJob.GENERATOR_COUNT_MODE, GeneratorJob.GENERATOR_COUNT_VALUE_DOMAIN);

    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    ArrayList<URLWebPage> fetchList = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK, FIELDS);

    // verify we got right amount of records
    assertEquals(1, fetchList.size());

    myConfiguration = new Configuration(myConfiguration);
    myConfiguration.setInt(GeneratorJob.GENERATOR_MAX_COUNT, 2);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK, FIELDS);

    // verify we got right amount of records
    assertEquals(3, fetchList.size()); // 2 + 1 skipped (already generated)

    myConfiguration = new Configuration(myConfiguration);
    myConfiguration.setInt(GeneratorJob.GENERATOR_MAX_COUNT, 3);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK, FIELDS);

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
  @Ignore("Temporarily diable until NUTCH-1572 is addressed.")
  public void testFilter() throws IOException, Exception {

    ArrayList<URLWebPage> list = new ArrayList<URLWebPage>();

    list.add(createURLWebPage("http://www.example.com/index.html", 1, 1));
    list.add(createURLWebPage("http://www.example.net/index.html", 1, 1));
    list.add(createURLWebPage("http://www.example.org/index.html", 1, 1));

    for (URLWebPage uwp : list) {
      webPageStore.put(TableUtil.reverseUrl(uwp.getUrl()), uwp.getDatum());
    }
    webPageStore.flush();

    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.set("urlfilter.suffix.file", "filter-all.txt");

    generateFetchlist(Integer.MAX_VALUE, myConfiguration, true);

    ArrayList<URLWebPage> fetchList = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK, FIELDS);

    assertEquals(0, fetchList.size());

    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = CrawlTestUtil.readContents(webPageStore, Mark.GENERATE_MARK, FIELDS);

    // verify nothing got filtered
    assertEquals(list.size(), fetchList.size());

  }

  /**
   * Generate Fetchlist.
   *
   * @param numResults
   *          number of results to generate
   * @param config
   *          Configuration to use
   * @return path to generated batch
   * @throws IOException
   */
  private void generateFetchlist(int numResults, Configuration config,
      boolean filter) throws Exception {
    // generate batch
    GeneratorJob g = new GeneratorJob();
    g.setConf(config);
    String batchId = g.generate(numResults, System.currentTimeMillis(), filter, false);
    if (batchId == null)
      throw new RuntimeException("Generator failed");
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
    WebPage page = new WebPage();
    page.setFetchInterval(fetchInterval);
    page.setScore(score);
    page.setStatus(CrawlStatus.STATUS_UNFETCHED);
    return new URLWebPage(url, page);
  }

}
