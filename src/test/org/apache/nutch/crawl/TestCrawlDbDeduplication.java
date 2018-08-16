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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapFile.Writer.Option;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    args[1] = "compareOrder";
    args[2] = "fetchTime,urlLength,score";
    int result = ToolRunner.run(conf, new DeduplicationJob(), args);
    Assert.assertEquals("DeduplicationJob did not succeed", 0, result);
    reader = new CrawlDbReader();
    String url1 = "http://nutch.apache.org/";
    String url2 = "https://nutch.apache.org/";
    CrawlDatum datum1 = reader.get(testCrawlDb.toString(), url1, conf);
    CrawlDatum datum2 = reader.get(testCrawlDb.toString(), url2, conf);
    Assert.assertNotNull("No CrawlDatum found in CrawlDb for " + url1, datum1);
    Assert.assertNotNull("No CrawlDatum found in CrawlDb for " + url2, datum2);
    // url1 has been fetched earlier, so it should "survive" as "db_fetched":
    Assert.assertEquals(CrawlDatum.STATUS_DB_FETCHED, datum1.getStatus());
    Assert.assertEquals(CrawlDatum.STATUS_DB_DUPLICATE, datum2.getStatus());
    reader.close();
    fs.delete(testCrawlDb, true);
  }

  @Test
  public void testDedupRedirects() throws Exception {
    String[] args = new String[3];
    args[0] = testCrawlDb.toString();
    args[1] = "compareOrder";
    args[2] = "fetchTime,urlLength,score";
    int result = ToolRunner.run(conf, new DedupRedirectsJob(), args);
    Assert.assertEquals("DedupRedirectsJob did not succeed", 0, result);
    reader = new CrawlDbReader();
    // url3 was fetched with status 200, so it should "survive" as "db_fetched"
    // while url1 and url2 both redirect to url3 and should be duplicates
    String url1 = "http://en.wikipedia.org/wiki/URL_redirection";
    String url2 = "https://www.wikipedia.org/wiki/URL_redirection";
    String url3 = "https://en.wikipedia.org/wiki/URL_redirection";
    checkStatus(url1, CrawlDatum.STATUS_DB_DUPLICATE);
    checkStatus(url2, CrawlDatum.STATUS_DB_DUPLICATE);
    checkStatus(url3, CrawlDatum.STATUS_DB_FETCHED);
    // url4: redirect points to a redirect -> mark as duplicate
    String url4 = "https://wikipedia.org/wiki/URL_redirection";
    checkStatus(url4, CrawlDatum.STATUS_DB_DUPLICATE);
    // url5: points to redirect not in CrawlDb
    // => leave as redirect in CrawlDb for now
    String url5 = "https:/wikipedia.org/wiki/URL_forwarding";
    checkStatus(url5, CrawlDatum.STATUS_DB_REDIR_PERM);
    reader.close();
    fs.delete(testCrawlDb, true);
  }

  private void checkStatus(String url, byte status) throws IOException {
    CrawlDatum datum = reader.get(testCrawlDb.toString(), url, conf);
    Assert.assertNotNull("No CrawlDatum found in CrawlDb for " + url, datum);
    Assert.assertEquals(
        "Exspected status for " + url + ": " + CrawlDatum.getStatusName(status),
        status, datum.getStatus());
  }

}
