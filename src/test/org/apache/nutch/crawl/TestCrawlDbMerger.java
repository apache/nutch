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

package org.apache.nutch.crawl;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapFile.Writer.Option;
import org.apache.hadoop.mapreduce.Job;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCrawlDbMerger {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  String url10 = "http://example.com/";
  String url11 = "http://example.com/foo";
  String url20 = "http://example.com/";
  String url21 = "http://example.com/bar";
  String[] urls_expected = new String[] { url10, url11, url21 };

  TreeSet<String> init1 = new TreeSet<String>();
  TreeSet<String> init2 = new TreeSet<String>();
  HashMap<String, CrawlDatum> expected = new HashMap<String, CrawlDatum>();
  CrawlDatum cd1, cd2, cd3;
  Configuration conf;
  FileSystem fs;
  Path testDir;
  CrawlDbReader reader;

  @Before
  public void setUp() throws Exception {
    init1.add(url10);
    init1.add(url11);
    init2.add(url20);
    init2.add(url21);
    long time = System.currentTimeMillis();
    cd1 = new CrawlDatum();
    cd1.setFetchInterval(1.0f);
    cd1.setFetchTime(time);
    cd1.getMetaData().put(new Text("name"), new Text("cd1"));
    cd1.getMetaData().put(new Text("cd1"), new Text("cd1"));
    cd2 = new CrawlDatum();
    cd2.setFetchInterval(1.0f);
    cd2.setFetchTime(time + 10000);
    cd2.getMetaData().put(new Text("name"), new Text("cd2"));
    cd3 = new CrawlDatum();
    cd3.setFetchInterval(1.0f);
    cd3.setFetchTime(time + 10000);
    cd3.getMetaData().putAll(cd1.getMetaData());
    cd3.getMetaData().putAll(cd2.getMetaData());
    expected.put(url10, cd3);
    expected.put(url11, cd1);
    expected.put(url21, cd2);
    conf = NutchConfiguration.create();
    fs = FileSystem.get(conf);
    testDir = new Path("test-crawldb-" + new java.util.Random().nextInt());
    fs.mkdirs(testDir);
  }

  @After
  public void tearDown() {
    try {
      if (fs.exists(testDir))
        fs.delete(testDir, true);
    } catch (Exception e) {
    }
    try {
      reader.close();
    } catch (Exception e) {
    }
  }

  /**
   * Test creates two sample {@link org.apache.nutch.crawl.CrawlDb}'s
   * populating entries for keys as {@link org.apache.hadoop.io.Text} e.g. URLs 
   * and values as {@link org.apache.nutch.crawl.CrawlDatum} e.g. record data. 
   * It then simulates a merge process for the two CrawlDb's via the {@link org.apache.nutch.crawl.CrawlDbMerger}
   * tool. The merged CrawlDb is then written to an arbitrary output location and the results
   * read using the {@link org.apache.nutch.crawl.CrawlDbReader} tool. 
   * Test assertions include comparing expected CrawlDb key, value (URL, CrawlDatum) values
   * with actual results based on the merge process. 
   * @throws Exception
   */
  @Test
  public void testMerge() throws Exception {
    Path crawldb1 = new Path(testDir, "crawldb1");
    Path crawldb2 = new Path(testDir, "crawldb2");
    Path output = new Path(testDir, "output");
    createCrawlDb(conf, fs, crawldb1, init1, cd1);
    createCrawlDb(conf, fs, crawldb2, init2, cd2);
    CrawlDbMerger merger = new CrawlDbMerger(conf);
    LOG.debug("* merging crawldbs to " + output);
    merger.merge(output, new Path[] { crawldb1, crawldb2 }, false, false);
    LOG.debug("* reading crawldb: " + output);
    reader = new CrawlDbReader();
    String crawlDb = output.toString();
    Iterator<String> it = expected.keySet().iterator();
    while (it.hasNext()) {
      String url = it.next();
      LOG.debug("url=" + url);
      CrawlDatum cd = expected.get(url);
      CrawlDatum res = reader.get(crawlDb, url, conf);
      LOG.debug(" -> " + res);
      System.out.println("url=" + url);
      System.out.println(" cd " + cd);
      System.out.println(" res " + res);
      // may not be null
      Assert.assertNotNull(res);
      Assert.assertTrue(cd.equals(res));
    }
    reader.close();
    fs.delete(testDir, true);
  }

  private void createCrawlDb(Configuration config, FileSystem fs, Path crawldb,
      TreeSet<String> init, CrawlDatum cd) throws Exception {
    LOG.debug("* creating crawldb: " + crawldb);
    Path dir = new Path(crawldb, CrawlDb.CURRENT_NAME);
    
    Option wKeyOpt = MapFile.Writer.keyClass(Text.class);
    org.apache.hadoop.io.SequenceFile.Writer.Option wValueOpt = SequenceFile.Writer.valueClass(CrawlDatum.class);
    
    MapFile.Writer writer = new MapFile.Writer(config, new Path(dir,
        "part-r-00000"), wKeyOpt, wValueOpt);
    Iterator<String> it = init.iterator();
    while (it.hasNext()) {
      String key = it.next();
      writer.append(new Text(key), cd);
    }
    writer.close();
  }
}
