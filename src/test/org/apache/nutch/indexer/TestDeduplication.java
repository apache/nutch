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

package org.apache.nutch.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.crawl.DbUpdaterJob;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.InjectorJob;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.Duplicate;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.util.AbstractNutchTest;
import org.apache.nutch.util.CrawlTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Server;

import static org.junit.Assert.*;

public class TestDeduplication extends AbstractNutchTest {
  
  final static Path testdir = new Path("build/test/dedup-test");
  Path urlPath;
  Server server;
  String crawlId = "dedupCrawlId";
  String batchId = "dedupTestBatch1";
  DataStore<String, Duplicate> duplicateStore;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    duplicateStore = StorageUtils.createWebStore(conf, String.class,
        Duplicate.class);
    
    conf.set(Nutch.CRAWL_ID_KEY, crawlId);
    conf.set(GeneratorJob.BATCH_ID, batchId);
    conf.setBoolean(FetcherJob.PARSE_KEY, true);
    urlPath = new Path(testdir, "urls");
    server = CrawlTestUtil.getServer(conf.getInt("content.server.port", 50000),
        "build/test/data/fetch-test-site");
    server.start();
  }
  
  /**
   * Test a full crawl with deduplication
   *
   * @throws Exception
   */
  @Test
  public void test() throws Exception {

    // create seedlist
    ArrayList<String> urls = new ArrayList<String>();
    addUrl(urls, "pagea.html");
    addUrl(urls, "dup_of_pagea.html");
    CrawlTestUtil.generateSeedList(fs, urlPath, urls);

    // inject
    InjectorJob injector = new InjectorJob(conf);
    injector.inject(urlPath);

    // generate
    long time = System.currentTimeMillis();
    GeneratorJob generator = new GeneratorJob(conf);
    generator.generate(Long.MAX_VALUE, time, false, false, false);

    // fetch
    FetcherJob fetcher = new FetcherJob(conf);
    fetcher.fetch(batchId, 1, false, -1);
    
    // update
    DbUpdaterJob updater = new DbUpdaterJob(conf);
    updater.updateTable(crawlId, batchId);
    
    // deduplicate
    DeduplicatorJob deduplicator = new DeduplicatorJob(conf);
    deduplicator.deduplicate(batchId, crawlId);
    
    // check the duplicate entry store
    Query<String, Duplicate> query = duplicateStore.newQuery();
    query.setFields(Duplicate._ALL_FIELDS);
    Result<String, Duplicate> results = duplicateStore.execute(query);
    assertTrue(results.next());
    Duplicate duplicate = results.get();
    assertFalse(results.next());
    assertEquals(duplicate.getUrls().size(), 2);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    server.stop();
    fs.delete(testdir, true);
    super.tearDown();
  }
  
  /** 
   * Maps a webpage to the local Jetty server address so that it can 
   * be fetched as part of an arraylist
   */
  private void addUrl(ArrayList<String> urls, String page) {
    urls.add("http://127.0.0.1:" + server.getConnectors()[0].getPort() + "/"
        + page);
  }

}
