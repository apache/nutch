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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.nutch.crawl.CrawlDBTestUtil.URLCrawlDatum;
import org.apache.nutch.test.IntegrationTest;
import org.apache.nutch.util.NutchJob;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * CrawlDbFiltering test which tests for correct, error free url normalization
 * when the CrawlDB includes urls with <code>DB GONE</code> status and
 * <code>CRAWLDB_PURGE_404</code> is set to true.
 * 
 * @author lufeng
 */
public class TestCrawlDbFilter {
  Configuration conf;
  Path dbDir;
  Path newCrawlDb;
  final static Path testdir = new Path("build/test/crawldbfilter-test");
  FileSystem fs;

  @Before
  public void setUp() throws Exception {
    conf = CrawlDBTestUtil.createConfiguration();
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
   * Test url404Purging
   * 
   * @throws Exception
   */
  @Test
  @Category({IntegrationTest.class})
  public void testUrl404Purging() throws Exception {
    // create a CrawlDatum with DB GONE status
    ArrayList<URLCrawlDatum> list = new ArrayList<URLCrawlDatum>();
    list.add(new URLCrawlDatum(new Text("http://www.example.com"),
        new CrawlDatum(CrawlDatum.STATUS_DB_GONE, 0, 0.0f)));
    list.add(new URLCrawlDatum(new Text("http://www.example1.com"),
        new CrawlDatum(CrawlDatum.STATUS_DB_FETCHED, 0, 0.0f)));
    list.add(new URLCrawlDatum(new Text("http://www.example2.com"),
        new CrawlDatum(CrawlDatum.STATUS_DB_UNFETCHED, 0, 0.0f)));
    dbDir = new Path(testdir, "crawldb");
    newCrawlDb = new Path(testdir, "newcrawldb");
    // create crawldb
    CrawlDBTestUtil.createCrawlDb(conf, fs, dbDir, list);
    // set CRAWLDB_PURGE_404 to true
    conf.setBoolean(CrawlDb.CRAWLDB_PURGE_404, true);
    conf.setBoolean(CrawlDbFilter.URL_NORMALIZING, true);
    conf.setBoolean(CrawlDbFilter.URL_FILTERING, false);
    conf.setInt("urlnormalizer.loop.count", 2);
    JobConf job = new NutchJob(conf);
    job.setJobName("Test CrawlDbFilter");
    Path current = new Path(dbDir, "current");
    if (FileSystem.get(job).exists(current)) {
      FileInputFormat.addInputPath(job, current);
    }
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(CrawlDbFilter.class);
    job.setReducerClass(CrawlDbReducer.class);
    FileOutputFormat.setOutputPath(job, newCrawlDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);
    JobClient.runJob(job);

    Path fetchlist = new Path(new Path(newCrawlDb, "part-00000"), "data");

    ArrayList<URLCrawlDatum> l = readContents(fetchlist);

    // verify we got right amount of records
    Assert.assertEquals(2, l.size());
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
    Option fFile = SequenceFile.Reader.file(fetchlist);
    SequenceFile.Reader reader = new SequenceFile.Reader(conf, fFile);

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
}
