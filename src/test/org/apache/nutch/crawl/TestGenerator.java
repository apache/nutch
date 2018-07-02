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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.nutch.crawl.CrawlDBTestUtil.URLCrawlDatum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic generator test. 1. Insert entries in crawldb 2. Generates entries to
 * fetch 3. Verifies that number of generated urls match 4. Verifies that
 * highest scoring urls are generated
 * 
 */
public class TestGenerator {

  Configuration conf;

  Path dbDir;

  Path segmentsDir;

  FileSystem fs;

  final static Path testdir = new Path("build/test/generator-test");

  @Before
  public void setUp() throws Exception {
    conf = CrawlDBTestUtil.createContext().getConfiguration();
    fs = FileSystem.get(conf);
    fs.delete(testdir, true);
  }

  @After
  public void tearDown() {
    delete(testdir);
  }

  private void delete(Path p) {
    try {
      fs.delete(p, true);
    } catch (IOException e) {
    }
  }

  /**
   * Test that generator generates fetchlish ordered by score (desc).
   * 
   * @throws Exception
   */
  @Test
  public void testGenerateHighest() throws Exception {

    final int NUM_RESULTS = 2;

    ArrayList<URLCrawlDatum> list = new ArrayList<URLCrawlDatum>();

    for (int i = 0; i <= 100; i++) {
      list.add(createURLCrawlDatum("http://aaa/" + pad(i), 1, i));
    }

    createCrawlDB(list);

    Path generatedSegment = generateFetchlist(NUM_RESULTS, conf, false);

    Path fetchlist = new Path(new Path(generatedSegment,
        CrawlDatum.GENERATE_DIR_NAME), "part-r-00000");

    ArrayList<URLCrawlDatum> l = readContents(fetchlist);

    // sort urls by score desc
    Collections.sort(l, new ScoreComparator());

    // verify we got right amount of records
    Assert.assertEquals(NUM_RESULTS, l.size());

    // verify we have the highest scoring urls
    Assert.assertEquals("http://aaa/100", (l.get(0).url.toString()));
    Assert.assertEquals("http://aaa/099", (l.get(1).url.toString()));
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
  public class ScoreComparator implements Comparator<URLCrawlDatum> {

    public int compare(URLCrawlDatum tuple1, URLCrawlDatum tuple2) {
      if (tuple2.datum.getScore() - tuple1.datum.getScore() < 0) {
        return -1;
      }
      if (tuple2.datum.getScore() - tuple1.datum.getScore() > 0) {
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
  @Test
  public void testGenerateHostLimit() throws Exception {
    ArrayList<URLCrawlDatum> list = new ArrayList<URLCrawlDatum>();

    list.add(createURLCrawlDatum("http://www.example.com/index1.html", 1, 1));
    list.add(createURLCrawlDatum("http://www.example.com/index2.html", 1, 1));
    list.add(createURLCrawlDatum("http://www.example.com/index3.html", 1, 1));

    createCrawlDB(list);

    int maxPerHost = 1;
    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.setInt(Generator.GENERATOR_MAX_COUNT, maxPerHost);
    Path generatedSegment = generateFetchlist(Integer.MAX_VALUE,
        myConfiguration, false);

    Path fetchlistPath = new Path(new Path(generatedSegment,
        CrawlDatum.GENERATE_DIR_NAME), "part-r-00000");

    ArrayList<URLCrawlDatum> fetchList = readContents(fetchlistPath);

    // verify we got right amount of records
    int expectedFetchListSize = Math.min(maxPerHost, list.size());
    Assert.assertEquals("Failed to apply generate.max.count by host",
        expectedFetchListSize, fetchList.size());

    maxPerHost = 2;
    myConfiguration = new Configuration(conf);
    myConfiguration.setInt(Generator.GENERATOR_MAX_COUNT, maxPerHost);
    generatedSegment = generateFetchlist(Integer.MAX_VALUE, myConfiguration,
        false);

    fetchlistPath = new Path(new Path(generatedSegment,
        CrawlDatum.GENERATE_DIR_NAME), "part-r-00000");

    fetchList = readContents(fetchlistPath);

    // verify we got right amount of records
    expectedFetchListSize = Math.min(maxPerHost, list.size());
    Assert.assertEquals("Failed to apply generate.max.count by host",
        expectedFetchListSize, fetchList.size());

    maxPerHost = 3;
    myConfiguration = new Configuration(conf);
    myConfiguration.setInt(Generator.GENERATOR_MAX_COUNT, maxPerHost);
    generatedSegment = generateFetchlist(Integer.MAX_VALUE, myConfiguration,
        false);

    fetchlistPath = new Path(new Path(generatedSegment,
        CrawlDatum.GENERATE_DIR_NAME), "part-r-00000");

    fetchList = readContents(fetchlistPath);

    // verify we got right amount of records
    expectedFetchListSize = Math.min(maxPerHost, list.size());
    Assert.assertEquals("Failed to apply generate.max.count by host",
        expectedFetchListSize, fetchList.size());
  }

  /**
   * Test that generator obeys the property "generator.max.count" and
   * "generator.count.per.domain".
   * 
   * @throws Exception
   */
  @Test
  public void testGenerateDomainLimit() throws Exception {
    ArrayList<URLCrawlDatum> list = new ArrayList<URLCrawlDatum>();

    list.add(createURLCrawlDatum("http://a.example.com/index.html", 1, 1));
    list.add(createURLCrawlDatum("http://b.example.com/index.html", 1, 1));
    list.add(createURLCrawlDatum("http://c.example.com/index.html", 1, 1));

    createCrawlDB(list);

    int maxPerDomain = 1;
    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.setInt(Generator.GENERATOR_MAX_COUNT, maxPerDomain);
    myConfiguration.set(Generator.GENERATOR_COUNT_MODE,
        Generator.GENERATOR_COUNT_VALUE_DOMAIN);

    Path generatedSegment = generateFetchlist(Integer.MAX_VALUE,
        myConfiguration, false);

    Path fetchlistPath = new Path(new Path(generatedSegment,
        CrawlDatum.GENERATE_DIR_NAME), "part-r-00000");

    ArrayList<URLCrawlDatum> fetchList = readContents(fetchlistPath);

    // verify we got right amount of records
    int expectedFetchListSize = Math.min(maxPerDomain, list.size());
    Assert.assertEquals("Failed to apply generate.max.count by domain",
        expectedFetchListSize, fetchList.size());

    maxPerDomain = 2;
    myConfiguration = new Configuration(myConfiguration);
    myConfiguration.setInt(Generator.GENERATOR_MAX_COUNT, maxPerDomain);
    generatedSegment = generateFetchlist(Integer.MAX_VALUE, myConfiguration,
        false);

    fetchlistPath = new Path(new Path(generatedSegment,
        CrawlDatum.GENERATE_DIR_NAME), "part-r-00000");

    fetchList = readContents(fetchlistPath);

    // verify we got right amount of records
    expectedFetchListSize = Math.min(maxPerDomain, list.size());
    Assert.assertEquals("Failed to apply generate.max.count by domain",
        expectedFetchListSize, fetchList.size());

    maxPerDomain = 3;
    myConfiguration = new Configuration(myConfiguration);
    myConfiguration.setInt(Generator.GENERATOR_MAX_COUNT, maxPerDomain);
    generatedSegment = generateFetchlist(Integer.MAX_VALUE, myConfiguration,
        false);

    fetchlistPath = new Path(new Path(generatedSegment,
        CrawlDatum.GENERATE_DIR_NAME), "part-r-00000");

    fetchList = readContents(fetchlistPath);

    // verify we got right amount of records
    expectedFetchListSize = Math.min(maxPerDomain, list.size());
    Assert.assertEquals("Failed to apply generate.max.count by domain",
        expectedFetchListSize, fetchList.size());
  }

  /**
   * Test generator obeys the filter setting.
   * 
   * @throws Exception
   * @throws IOException
   */
  @Test
  public void testFilter() throws IOException, Exception {

    ArrayList<URLCrawlDatum> list = new ArrayList<URLCrawlDatum>();

    list.add(createURLCrawlDatum("http://www.example.com/index.html", 1, 1));
    list.add(createURLCrawlDatum("http://www.example.net/index.html", 1, 1));
    list.add(createURLCrawlDatum("http://www.example.org/index.html", 1, 1));

    createCrawlDB(list);

    Configuration myConfiguration = new Configuration(conf);
    myConfiguration.set("urlfilter.suffix.file", "filter-all.txt");

    Path generatedSegment = generateFetchlist(Integer.MAX_VALUE,
        myConfiguration, true);

    Assert.assertNull("should be null (0 entries)", generatedSegment);

    generatedSegment = generateFetchlist(Integer.MAX_VALUE, myConfiguration,
        false);

    Path fetchlistPath = new Path(new Path(generatedSegment,
        CrawlDatum.GENERATE_DIR_NAME), "part-r-00000");

    ArrayList<URLCrawlDatum> fetchList = readContents(fetchlistPath);

    // verify nothing got filtered
    Assert.assertEquals(list.size(), fetchList.size());

  }

  /**
   * Read contents of fetchlist.
   * 
   * @param fetchlist
   *          path to Generated fetchlist
   * @return Generated {@link URLCrawlDatum} objects
   * @throws IOException
   */
  private ArrayList<URLCrawlDatum> readContents(Path fetchlist)
      throws IOException {
    // verify results
    Option rFile = SequenceFile.Reader.file(fetchlist);
    SequenceFile.Reader reader = new SequenceFile.Reader(conf, rFile);

    ArrayList<URLCrawlDatum> l = new ArrayList<URLCrawlDatum>();

    READ: do {
      Text key = new Text();
      CrawlDatum value = new CrawlDatum();
      if (!reader.next(key, value)) {
        break READ;
      }
      l.add(new URLCrawlDatum(key, value));
    } while (true);

    reader.close();
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
   * @throws InterruptedException 
   * @throws ClassNotFoundException 
   */
  private Path generateFetchlist(int numResults, Configuration config,
      boolean filter) throws IOException, ClassNotFoundException, InterruptedException {
    // generate segment
    Generator g = new Generator(config);
    Path[] generatedSegment = g.generate(dbDir, segmentsDir, -1, numResults,
        Long.MAX_VALUE, filter, false);
    if (generatedSegment == null)
      return null;
    return generatedSegment[0];
  }

  /**
   * Creates CrawlDB.
   * 
   * @param list
   *          database contents
   * @throws IOException
   * @throws Exception
   */
  private void createCrawlDB(ArrayList<URLCrawlDatum> list) throws IOException,
      Exception {
    dbDir = new Path(testdir, "crawldb");
    segmentsDir = new Path(testdir, "segments");
    fs.mkdirs(dbDir);
    fs.mkdirs(segmentsDir);

    // create crawldb
    CrawlDBTestUtil.createCrawlDb(conf, fs, dbDir, list);
  }

  /**
   * Constructs new {@link URLCrawlDatum} from submitted parameters.
   * 
   * @param url
   *          url to use
   * @param fetchInterval
   *          {@link CrawlDatum#setFetchInterval(float)}
   * @param score
   *          {@link CrawlDatum#setScore(float)}
   * @return Constructed object
   */
  private URLCrawlDatum createURLCrawlDatum(final String url,
      final int fetchInterval, final float score) {
    return new CrawlDBTestUtil.URLCrawlDatum(new Text(url), new CrawlDatum(
        CrawlDatum.STATUS_DB_UNFETCHED, fetchInterval, score));
  }
}
