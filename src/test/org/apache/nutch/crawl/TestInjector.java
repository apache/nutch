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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.TableUtil;
import org.gora.hbase.store.HBaseStore;
import org.gora.query.Query;
import org.gora.query.Result;
import org.gora.store.DataStore;
import org.gora.store.DataStoreFactory;
import org.junit.Before;

/**
 * Basic injector test: 1. Creates a text file with urls 2. Injects them into
 * crawldb 3. Reads crawldb entries and verifies contents 4. Injects more urls
 * into webdb 5. Reads crawldb entries and verifies contents
 * 
 * @author nutch-dev <nutch-dev at lucene.apache.org>
 */
public class TestInjector extends HBaseClusterTestCase {

  private Configuration conf;
  private FileSystem fs;
  final static Path testdir = new Path("build/test/inject-test");
  private DataStore<String, WebPage> webPageStore;
  Path urlPath;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    conf = CrawlDBTestUtil.createConfiguration();
    urlPath = new Path(testdir, "urls");
    fs = FileSystem.get(conf);
    if (fs.exists(urlPath))
      fs.delete(urlPath, false);
    webPageStore = DataStoreFactory.getDataStore(HBaseStore.class,
        String.class, WebPage.class);
  }

  @Override
  public void tearDown() throws Exception {
    fs.delete(testdir, true);
    webPageStore.close();
    super.tearDown();
  }

  public void testInject() throws Exception {
    ArrayList<String> urls = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      urls.add("http://zzz.com/" + i + ".html\tnutch.score=" + i
          + "\tcustom.attribute=" + i);
    }
    CrawlDBTestUtil.generateSeedList(fs, urlPath, urls);

    InjectorJob injector = new InjectorJob();
    injector.setConf(conf);
    injector.inject(urlPath);

    // verify results
    List<String> read = readCrawldb();

    Collections.sort(read);
    Collections.sort(urls);

    assertEquals(urls.size(), read.size());

    assertTrue(urls.containsAll(read));
    assertTrue(read.containsAll(urls));

    // inject more urls
    ArrayList<String> urls2 = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      urls2.add("http://xxx.com/" + i + ".html");
    }
    CrawlDBTestUtil.generateSeedList(fs, urlPath, urls2);
    injector.inject(urlPath);
    urls.addAll(urls2);

    // verify results
    read = readCrawldb();

    Collections.sort(read);
    Collections.sort(urls);

    assertEquals(urls.size(), read.size());

    assertTrue(read.containsAll(urls));
    assertTrue(urls.containsAll(read));

  }

  /**
   * Read from a Gora datastore + make sure we get the score and custom metadata
   * 
   * @throws ClassNotFoundException
   **/
  private List<String> readCrawldb() throws Exception {
    ArrayList<String> read = new ArrayList<String>();

    Query<String, WebPage> query = webPageStore.newQuery();
    Result<String, WebPage> result = webPageStore.execute(query);

    while (result.next()) {
      String skey = result.getKey();
      WebPage page = result.get();
      float fscore = page.getScore();
      String representation = TableUtil.unreverseUrl(skey);
      ByteBuffer bb = page.getFromMetadata(new Utf8("custom.attribute"));
      if (bb != null) {
        representation += "\tnutch.score=" + (int) fscore;
        representation += "\tcustom.attribute=" + Bytes.toString(bb.array());
      }
      read.add(representation);
    }
    result.close();
    return read;
  }

}
