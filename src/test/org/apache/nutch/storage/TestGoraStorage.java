/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.storage;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.AbstractNutchTest;
import org.apache.nutch.util.CrawlTestUtil;
import org.hsqldb.Server;
import org.junit.Ignore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests basic Gora functionality by writing and reading webpages.
 */
public class TestGoraStorage extends AbstractNutchTest {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  /**
   * Sequentially read and write pages to a store.
   * 
   * @throws Exception
   */
  @Test
  public void testSinglethreaded() throws Exception {
    String id = "singlethread";
    readWrite(id, webPageStore);
  }

  private static void readWrite(String id, DataStore<String, WebPage> store) 
      throws IOException, Exception {
    WebPage page = new WebPage();
    int max = 1000;
    for (int i = 0; i < max; i++) {
      // store a page with title
      String key = "key-" + id + "-" + i;
      String title = "title" + i;
      page.setTitle(new Utf8(title));
      store.put(key, page);
      store.flush();

      // retrieve page and check title
      page = store.get(key);
      assertNotNull(page);
      assertEquals(title, page.getTitle().toString());
    }

    // scan over the rows
    Result<String, WebPage> result = store.execute(store.newQuery());
    int count = 0;
    while (result.next()) {
      try {
        // only count keys in the store for the current id
        if (result.getKey().contains(id))
          count++;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    // check amount
    assertEquals(max, count);
  }

  /**
   * Tests multiple thread reading and writing to the same store, this should be
   * no problem because {@link DataStore} implementations claim to be thread
   * safe.
   * 
   * @throws Exception
   */
  @Test
  @Ignore("Temporarily diable until NUTCH-1572 is addressed.")
  public void testMultithreaded() throws Exception {
    // create a fixed thread pool
    int numThreads = 8;
    ExecutorService pool = Executors.newFixedThreadPool(numThreads);

    // define a list of tasks
    Collection<Callable<Integer>> tasks = new ArrayList<Callable<Integer>>();
    for (int i = 0; i < numThreads; i++) {
      tasks.add(new Callable<Integer>() {
        @Override
        public Integer call() {
          try {
            // run a sequence
            readWrite(Thread.currentThread().getName(), webPageStore);
            // everything ok, return 0
            return 0;
          } catch (Exception e) {
            e.printStackTrace();
            // this will fail the test
            return 1;
          }
        }
      });
    }

    // submit them at once
    List<Future<Integer>> results = pool.invokeAll(tasks);

    // check results
    for (Future<Integer> result : results) {
      assertEquals(0, (int) result.get());
    }
  }
  
  /**
   * Tests multiple processes reading and writing to the same store backend, 
   * this is to simulate a multi process Nutch environment (i.e. MapReduce).
   * 
   * @throws Exception
   */
  @Test
  public void testMultiProcess() throws Exception {
    // create and start a hsql server, a stand-alone (memory backed) db
    // (important: a stand-alone server should be used because simple
    //  file based access i.e. jdbc:hsqldb:file is NOT process-safe.)
    Server server = new Server();
    server.setDaemon(true);
    server.setSilent(true); // disables LOTS of trace
    final String className = getClass().getName();
    String dbName = "test";
    server.setDatabasePath(0, "mem:"+dbName);
    server.setDatabaseName(0, dbName);
    server.start();
    
    //create the store so that the tests can start right away
    StorageUtils.createWebStore(conf, String.class, WebPage.class);
    
    // create a fixed thread pool
    int numThreads = 4;
    ExecutorService pool = Executors.newFixedThreadPool(numThreads);
    
    // spawn multiple processes, each thread spawns own process
    Collection<Callable<Integer>> tasks = new ArrayList<Callable<Integer>>();
    for (int i = 0; i < numThreads; i++) {
      tasks.add(new Callable<Integer>() {
        @Override
        public Integer call() {
          try {
            String separator = System.getProperty("file.separator");
            String classpath = System.getProperty("java.class.path");
            String pathSeparator = System.getProperty("path.separator");
            // connect local sql service
            classpath = "./src/testprocess" + pathSeparator + classpath;
            String path = System.getProperty("java.home") + separator + "bin"
                + separator + "java";
            ProcessBuilder processBuilder = new ProcessBuilder(path, "-cp", 
                classpath, className);
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();
            InputStream in = process.getInputStream();
            int exit = process.waitFor();
            //print the output of the process
            System.out.println("===Process stream for " + Thread.currentThread() 
                + "\n" + IOUtils.toString(in) + "===End of process stream.");
            in.close();
            // process should exit with zero code
            return exit;
          } catch (Exception e) {
            e.printStackTrace();
            // this will fail the test
            return 1;
          }
        }
      });
    }

    // submit them at once
    List<Future<Integer>> results = pool.invokeAll(tasks);

    // check results
    for (Future<Integer> result : results) {
      assertEquals(0, (int) result.get());
    }
    
    //stop db
    server.stop();
  }

  public static void main(String[] args) throws Exception {
    // entry point for the multiprocess test
    System.out.println("Starting!");

    Configuration localConf = CrawlTestUtil.createConfiguration();
    localConf.set("storage.data.store.class", "org.apache.gora.memory.store.MemStore");

    DataStore<String, WebPage> store = StorageUtils.createWebStore(localConf,
        String.class, WebPage.class);
    readWrite("single_id", store);
    System.out.println("Done.");
  }
}
