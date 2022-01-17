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

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCrawlDbDeduplication {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  Configuration conf;
  FileSystem fs;
  String testDir;
  Path testCrawlDb;
  CrawlDbReader reader;

  @Before
  public void setUp() throws Exception {
    conf = NutchConfiguration.create();
    fs = FileSystem.get(conf);
    testDir = "test-crawldb-" + new java.util.Random().nextInt();
    File sampleCrawlDb = new File(System.getProperty("test.build.data", "."),
        "deduplication-crawldb");
    LOG.info("Copying CrawlDb {} into test directory {}", sampleCrawlDb,
        testDir);
    FileUtils.copyDirectory(sampleCrawlDb, new File(testDir));
    testCrawlDb = new Path(testDir);
    for (FileStatus s : fs.listStatus(testCrawlDb)) {
      LOG.info("{}", s);
    }
    reader = new CrawlDbReader();
  }

  @After
  public void tearDown() {
    try {
      if (fs.exists(testCrawlDb))
        fs.delete(testCrawlDb, true);
    } catch (Exception e) {
    }
    try {
      reader.close();
    } catch (Exception e) {
    }
  }

  @Test
  public void testDeduplication() throws Exception {
    String[] args = new String[3];
    args[0] = testCrawlDb.toString();
    args[1] = "-compareOrder";
    args[2] = "fetchTime,urlLength,score";
    int result = ToolRunner.run(conf, new DeduplicationJob(), args);
    Assert.assertEquals("DeduplicationJob did not succeed", 0, result);
    String url1 = "http://nutch.apache.org/";
    String url2 = "https://nutch.apache.org/";
    // url1 has been fetched earlier, so it should "survive" as "db_fetched":
    checkStatus(url1, CrawlDatum.STATUS_DB_FETCHED);
    checkStatus(url2, CrawlDatum.STATUS_DB_DUPLICATE);
  }

  @Test
  public void testDeduplicationHttpsOverHttp() throws Exception {
    String[] args = new String[3];
    args[0] = testCrawlDb.toString();
    args[1] = "-compareOrder";
    args[2] = "httpsOverHttp,fetchTime,urlLength,score";
    int result = ToolRunner.run(conf, new DeduplicationJob(), args);
    Assert.assertEquals("DeduplicationJob did not succeed", 0, result);
    String url1 = "http://nutch.apache.org/";
    String url2 = "https://nutch.apache.org/";
    // url2 is https://, so it should "survive" as "db_fetched":
    checkStatus(url1, CrawlDatum.STATUS_DB_DUPLICATE);
    checkStatus(url2, CrawlDatum.STATUS_DB_FETCHED);
  }

  private void checkStatus(String url, byte status) throws IOException {
    CrawlDatum datum = reader.get(testCrawlDb.toString(), url, conf);
    Assert.assertNotNull("No CrawlDatum found in CrawlDb for " + url, datum);
    Assert.assertEquals(
        "Expected status for " + url + ": " + CrawlDatum.getStatusName(status),
        status, datum.getStatus());
  }

  static class TestDedupReducer extends DeduplicationJob.DedupReducer<Text> {

    void setCompareOrder(String compareOrder) {
      this.compareOrder = compareOrder.split(",");
    }

    String getDuplicate(String one, String two) {
      CrawlDatum d1 = new CrawlDatum();
      d1.getMetaData().put(DeduplicationJob.urlKey, new Text(one));
      CrawlDatum d2 = new CrawlDatum();
      d2.getMetaData().put(DeduplicationJob.urlKey, new Text(two));
      CrawlDatum dup = getDuplicate(d1, d2);
      if (dup == null) {
        return null;
      }
      return dup.getMetaData().get(DeduplicationJob.urlKey).toString();
    }
  }

  public String getDuplicateURL(String compareOrder, String url1, String url2) {
    TestDedupReducer dedup = new TestDedupReducer();
    dedup.setCompareOrder(compareOrder);
    return dedup.getDuplicate(url1, url2);
  }

  @Test
  public void testCompareURLs() {
    // test same protocol, same length: no decision possible
    String url0 = "https://example.com/";
    Assert.assertNull(getDuplicateURL("httpsOverHttp,urlLength", url0, url0));
    String url1 = "http://nutch.apache.org/";
    String url2 = "https://nutch.apache.org/";
    // test httpsOverHttp
    Assert.assertEquals(url1, getDuplicateURL("httpsOverHttp", url1, url2));
    // test urlLength
    Assert.assertEquals(url2, getDuplicateURL("urlLength", url1, url2));
    // test urlLength with percent-encoded URLs
    // "b%C3%BCcher" (unescaped "bücher") is shorter than "buecher"
    String url3 = "https://example.com/b%C3%BCcher";
    String url4 = "https://example.com/buecher";
    Assert.assertEquals(url4, getDuplicateURL("urlLength", url3, url4));
    // test NUTCH-2935: should not throw error on invalid percent-encoding
    String url5 = "https://example.com/%YR";
    String url6 = "https://example.com/%YR%YR";
    Assert.assertEquals(url6, getDuplicateURL("urlLength", url5, url6));
  }

}
