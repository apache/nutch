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

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.gora.hbase.store.HBaseStore;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.store.DataStore;
import org.gora.store.DataStoreFactory;

/**
 * Basic generator test. 1. Insert entries in webtable 2. Generates entries to
 * fetch 3. Verifies that number of generated urls match 4. Verifies that
 * highest scoring urls are generated
 *
 * @author nutch-dev <nutch-dev at lucene.apache.org>
 * @param <URLWebPage>
 *
 */
public class TestGenerator extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestGenerator.class);

  Configuration conf;

  private DataStore<String, WebPage> webPageStore;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    conf = CrawlDBTestUtil.createConfiguration();
    DataStoreFactory.properties.setProperty("gora.sqlstore.jdbc.driver","org.hsqldb.jdbcDriver");
    DataStoreFactory.properties.setProperty("gora.gora.sqlstore.jdbc.url","jdbc:hsqldb:hsql://localhost/nutchtest");
    webPageStore = DataStoreFactory.getDataStore(HBaseStore.class,
        String.class, WebPage.class);
  }

  @Override
  protected void tearDown() throws Exception {
    webPageStore.close();
    super.tearDown();
  }

  /**
   * Test that generator generates fetchlish ordered by score (desc).
   *
   * @throws Exception
   */
  public void testGenerateHighest() throws Exception {

    final int NUM_RESULTS = 2;

    ArrayList<URLWebPage> list = new ArrayList<URLWebPage>();

    for (int i = 0; i <= 100; i++) {
      list.add(createURLWebPage("http://aaa/" + pad(i), 1, i));
    }

    for (URLWebPage uwp : list) {
      webPageStore.put(TableUtil.reverseUrl(uwp.getUrl()), uwp.getDatum());
    }

    generateFetchlist(NUM_RESULTS, conf, false);

    ArrayList<URLWebPage> l = readContents();

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
   * Test that generator obeys the property "generate.max.per.host".
   *
   * @throws Exception
   */
  public void testGenerateHostLimit() throws Exception {
    ArrayList<URLWebPage> list = new ArrayList<URLWebPage>();

    list.add(createURLWebPage("http://www.example.com/index1.html", 1, 1));
    list.add(createURLWebPage("http://www.example.com/index2.html", 1, 1));
    list.add(createURLWebPage("http://www.example.com/index3.html", 1, 1));

    for (URLWebPage uwp : list) {
      webPageStore.put(TableUtil.reverseUrl(uwp.getUrl()), uwp.getDatum());
    }

    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.setInt(GeneratorJob.GENERATE_MAX_PER_HOST, 1);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    ArrayList<URLWebPage> fetchList = readContents();

    // verify we got right amount of records
    assertEquals(1, fetchList.size());

    myConfiguration = new Configuration(conf);
    myConfiguration.setInt(GeneratorJob.GENERATE_MAX_PER_HOST, 2);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = readContents();

    // verify we got right amount of records
    assertEquals(2, fetchList.size());

    myConfiguration = new Configuration(conf);
    myConfiguration.setInt(GeneratorJob.GENERATE_MAX_PER_HOST, 3);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = readContents();

    // verify we got right amount of records
    assertEquals(3, fetchList.size());
  }

  /**
   * Test that generator obeys the property "generate.max.per.host" and
   * "generate.max.per.host.by.ip".
   *
   * @throws Exception
   */
  public void toastGenerateHostIPLimit() throws Exception {
    ArrayList<URLWebPage> list = new ArrayList<URLWebPage>();

    list.add(createURLWebPage("http://www.example.com/index.html", 1, 1));
    list.add(createURLWebPage("http://www.example.net/index.html", 1, 1));
    list.add(createURLWebPage("http://www.example.org/index.html", 1, 1));

    for (URLWebPage uwp : list) {
      webPageStore.put(TableUtil.reverseUrl(uwp.getUrl()), uwp.getDatum());
    }

    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.setInt(GeneratorJob.GENERATE_MAX_PER_HOST, 1);
    myConfiguration.setBoolean(GeneratorJob.GENERATE_MAX_PER_HOST_BY_IP, true);

    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    ArrayList<URLWebPage> fetchList = readContents();

    // verify we got right amount of records
    assertEquals(1, fetchList.size());

    myConfiguration = new Configuration(myConfiguration);
    myConfiguration.setInt(GeneratorJob.GENERATE_MAX_PER_HOST, 2);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = readContents();

    // verify we got right amount of records
    assertEquals(2, fetchList.size());

    myConfiguration = new Configuration(myConfiguration);
    myConfiguration.setInt(GeneratorJob.GENERATE_MAX_PER_HOST, 3);
    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = readContents();

    // verify we got right amount of records
    assertEquals(3, fetchList.size());
  }

  /**
   * Test generator obeys the filter setting.
   *
   * @throws Exception
   * @throws IOException
   */
  public void testFilter() throws IOException, Exception {

    ArrayList<URLWebPage> list = new ArrayList<URLWebPage>();

    list.add(createURLWebPage("http://www.example.com/index.html", 1, 1));
    list.add(createURLWebPage("http://www.example.net/index.html", 1, 1));
    list.add(createURLWebPage("http://www.example.org/index.html", 1, 1));

    for (URLWebPage uwp : list) {
      webPageStore.put(TableUtil.reverseUrl(uwp.getUrl()), uwp.getDatum());
    }

    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.set("urlfilter.suffix.file", "filter-all.txt");

    generateFetchlist(Integer.MAX_VALUE, myConfiguration, true);

    ArrayList<URLWebPage> fetchList = readContents();

    assertEquals(0, fetchList.size());

    generateFetchlist(Integer.MAX_VALUE, myConfiguration, false);

    fetchList = readContents();

    // verify nothing got filtered
    assertEquals(list.size(), fetchList.size());

  }

  /**
   * Read entries marked as fetchable
   *
   * @return Generated {@link URLWebPage} objects
   * @throws IOException
   */
  private ArrayList<URLWebPage> readContents() throws IOException {
    ArrayList<URLWebPage> l = new ArrayList<URLWebPage>();

    Query<String, WebPage> query = webPageStore.newQuery();
    query.setFields(WebPage.Field.MARKERS.getName());

    Result<String, WebPage> results = webPageStore.execute(query);

    while (results.next()) {
      WebPage page = results.get();
      String url = results.getKey();
      LOG.info("FOUND IN TABLE :" + url);

      if (page == null)
        continue;

      if (Mark.GENERATE_MARK.checkMark(page) == null)
        continue;

      // it has been marked as generated so it is ready for the fetch
      l.add(new URLWebPage(TableUtil.unreverseUrl(url), page));
    }

    return l;
  }

  /**
   * Generate Fetchlist.
   *
   * @param numResults
   *          number of results to generate
   * @param config
   *          Configuration to use
   * @return path to generated segment
   * @throws IOException
   */
  private void generateFetchlist(int numResults, Configuration config,
      boolean filter) throws Exception {
    // generate segment
    GeneratorJob g = new GeneratorJob();
    g.setConf(config);
    String crawlId = g.generate(numResults, Long.MAX_VALUE, filter, false);
    if (crawlId == null)
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
