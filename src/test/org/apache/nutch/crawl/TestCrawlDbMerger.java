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

import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class TestCrawlDbMerger extends TestCase {
  private static final Logger LOG = Logger.getLogger(CrawlDbMerger.class.getName());
  
  String url10 = "http://example.com/";
  String url11 = "http://example.com/foo";
  String url20 = "http://example.com/";
  String url21 = "http://example.com/bar";
  String[] urls_expected = new String[] {
          url10,
          url11,
          url21
  };
  
  TreeSet init1 = new TreeSet();
  TreeSet init2 = new TreeSet();
  HashMap expected = new HashMap();
  CrawlDatum cd1, cd2, cd3;
  Configuration conf;
  FileSystem fs;
  Path testDir;
  CrawlDbReader reader;
  
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
    testDir = new Path("test-crawldb-" +
            new java.util.Random().nextInt());
    fs.mkdirs(testDir);
  }
  
  public void tearDown() {
    try {
      if (fs.exists(testDir))
        fs.delete(testDir);
    } catch (Exception e) { }
    try {
      reader.close();
    } catch (Exception e) { }
  }

  public void testMerge() throws Exception {
    Path crawldb1 = new Path(testDir, "crawldb1");
    Path crawldb2 = new Path(testDir, "crawldb2");
    Path output = new Path(testDir, "output");
    createCrawlDb(conf, fs, crawldb1, init1, cd1);
    createCrawlDb(conf, fs, crawldb2, init2, cd2);
    CrawlDbMerger merger = new CrawlDbMerger(conf);
    LOG.fine("* merging crawldbs to " + output);
    merger.merge(output, new Path[]{crawldb1, crawldb2}, false, false);
    LOG.fine("* reading crawldb: " + output);
    reader = new CrawlDbReader();
    String crawlDb = output.toString();
    Iterator it = expected.keySet().iterator();
    while (it.hasNext()) {
      String url = (String)it.next();
      LOG.fine("url=" + url);
      CrawlDatum cd = (CrawlDatum)expected.get(url);
      CrawlDatum res = reader.get(crawlDb, url, conf);
      LOG.fine(" -> " + res);
      System.out.println("url=" + url);
      System.out.println(" cd " + cd);
      System.out.println(" res " + res);
      // may not be null
      assertNotNull(res);
      assertTrue(cd.equals(res));
    }
    reader.close();
    fs.delete(testDir);
  }
  
  private void createCrawlDb(Configuration config, FileSystem fs, Path crawldb, TreeSet init, CrawlDatum cd) throws Exception {
    LOG.fine("* creating crawldb: " + crawldb);
    Path dir = new Path(crawldb, CrawlDb.CURRENT_NAME);
    MapFile.Writer writer = new MapFile.Writer(config, fs, new Path(dir, "part-00000").toString(), Text.class, CrawlDatum.class);
    Iterator it = init.iterator();
    while (it.hasNext()) {
      String key = (String)it.next();
      writer.append(new Text(key), cd);
    }
    writer.close();
  }
}
