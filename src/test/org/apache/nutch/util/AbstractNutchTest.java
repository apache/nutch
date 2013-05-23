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
package org.apache.nutch.util;

import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;

/**
 * This class provides common routines for setup/teardown of an in-memory data
 * store.
 */
public class AbstractNutchTest {

  protected Configuration conf;
  protected FileSystem fs;
  protected Path testdir = new Path("build/test/working");
  protected DataStore<String, WebPage> webPageStore;
  protected boolean persistentDataStore = false;

  public void setUp() throws Exception {
    conf = CrawlTestUtil.createConfiguration();
    conf.set("storage.data.store.class", "org.apache.gora.memory.store.MemStore");
    fs = FileSystem.get(conf);
    webPageStore = StorageUtils.createWebStore(conf, String.class,
        WebPage.class);
  }

  public void tearDown() throws Exception {
    // empty the database after test
    if (!persistentDataStore) {
      webPageStore.deleteByQuery(webPageStore.newQuery());
      webPageStore.flush();
      webPageStore.close();
    }
    fs.delete(testdir, true);
  }

}
