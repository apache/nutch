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

import java.io.File;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;

import junit.framework.TestCase;

public class TestGoraStorage extends TestCase {
  Configuration conf;
  
  public void init() throws Exception {
    conf = NutchConfiguration.create();
  }
  
  public void setUp() throws Exception {
    conf = NutchConfiguration.create();
    DataStore<String,WebPage> store;
    
    store = StorageUtils.createDataStore(conf, String.class, WebPage.class);
    store.deleteByQuery(store.newQuery());
    store.close();
  }
  
  private class Worker extends Thread {
    DataStore<String,WebPage> store;
    WebPage page = new WebPage();
    int start, count, id;
    int reopenMark, reopenCount = 0;
    boolean reopens = false;
    
    public Worker(int id, int start, int count, boolean reopens) {
      this.id = id;
      this.start = start;
      this.count = count;
      reopenMark = new Random().nextInt(count / 4) + count / 4;
      this.reopens = reopens;
    }
    
    public void run() {
      threadCount.incrementAndGet();
      try {
        store = StorageUtils.createDataStore(conf, String.class, WebPage.class);
        for (int i = 0; i < count; i++) {
          if (i > 0 && ((count % 10) == 0)) {
            System.out.println(" -W" + id + "(" + i + "/" + count + ")");
          }
          if (reopens && (i > 0) && (i % reopenMark) == 0) {
            System.out.println(" -W" + id + " reopen " + (++reopenCount));
            store.flush();
            store.close();
            store = StorageUtils.createDataStore(conf, String.class, WebPage.class);
          }
          page.setTitle(new Utf8(String.valueOf(start + i)));
          store.put(String.valueOf(start + i), page);
          try {
            sleep(10);
          } catch (Exception e) {};
        }
        store.flush();
        store.close();
      } catch (Exception e) {
        fail(e.getMessage());
      }
      threadCount.decrementAndGet();
    }
  }
  
  private AtomicInteger threadCount = new AtomicInteger(0);
  
  public void testMultithread() throws Exception {
    int COUNT = 1000;
    int NUM = 100;
    DataStore<String,WebPage> store;
    
    for (int i = 0; i < NUM; i++) {
      Worker w = new Worker(i, i * COUNT, COUNT, true);
      w.start();
    }
    while (threadCount.get() > 0) {
      try {
        Thread.sleep(5000);
        System.out.println("-threads " + threadCount.get() + "/" + NUM);
      } catch (Exception e) {};
    }
    System.out.println("Verifying...");
    store = StorageUtils.createDataStore(conf, String.class, WebPage.class);
    Result<String,WebPage> res = store.execute(store.newQuery());
    int size = COUNT * NUM;
    BitSet keys = new BitSet(size);
    while (res.next()) {
      String key = res.getKey();
      WebPage p = res.get();
      assertEquals(key, p.getTitle().toString());
      int pos = Integer.parseInt(key);
      assertTrue(pos < size && pos >= 0);
      if (keys.get(pos)) {
        fail("key " + key + " already set!");
      }
      keys.set(pos);
    }
    assertEquals(size, keys.cardinality());
  }
  
  public void testMultiProcess() throws Exception {
    int COUNT = 1000;
    int NUM = 100;
    DataStore<String,WebPage> store;
    List<Process> procs = new ArrayList<Process>();
    
    for (int i = 0; i < NUM; i++) {
      Process p = launch(i, i * COUNT, COUNT);
      procs.add(p);
    }
    
    while (procs.size() > 0) {
      try {
        Thread.sleep(5000);
      } catch (Exception e) {};
      Iterator<Process> it = procs.iterator();
      while (it.hasNext()) {
        Process p = it.next();
        int code = 1;
        try {
          code = p.exitValue();
          assertEquals(0, code);
          it.remove();
          p.destroy();
        } catch (IllegalThreadStateException e) {
          // not ready yet
        }
      }
      System.out.println("* running " + procs.size() + "/" + NUM);
    }
    System.out.println("Verifying...");
    store = StorageUtils.createDataStore(conf, String.class, WebPage.class);
    Result<String,WebPage> res = store.execute(store.newQuery());
    int size = COUNT * NUM;
    BitSet keys = new BitSet(size);
    while (res.next()) {
      String key = res.getKey();
      WebPage p = res.get();
      assertEquals(key, p.getTitle().toString());
      int pos = Integer.parseInt(key);
      assertTrue(pos < size && pos >= 0);
      if (keys.get(pos)) {
        fail("key " + key + " already set!");
      }
      keys.set(pos);
    }
    if (size != keys.cardinality()) {
      System.out.println("ERROR Missing keys:");
      for (int i = 0; i < size; i++) {
        if (keys.get(i)) continue;
        System.out.println(" " + i);
      }
      fail("key count should be " + size + " but is " + keys.cardinality());
    }
  }
  
  private Process launch(int id, int start, int count) throws Exception {
    //  Build exec child jmv args.
    Vector<String> vargs = new Vector<String>(8);
    File jvm =                                  // use same jvm as parent
      new File(new File(System.getProperty("java.home"), "bin"), "java");

    vargs.add(jvm.toString());

    // Add child (task) java-vm options.
    // tmp dir
    String prop = System.getProperty("java.io.tmpdir");
    vargs.add("-Djava.io.tmpdir=" + prop);
    // library path
    prop = System.getProperty("java.library.path");
    if (prop != null) {
      vargs.add("-Djava.library.path=" + prop);      
    }
    // working dir
    prop = System.getProperty("user.dir");
    vargs.add("-Duser.dir=" + prop);    
    // combat the stupid Xerces issue
    vargs.add("-Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
    // prepare classpath
    String sep = System.getProperty("path.separator");
    StringBuffer classPath = new StringBuffer();
    // start with same classpath as parent process
    classPath.append(System.getProperty("java.class.path"));
    //classPath.append(sep);
    // Add classpath.
    vargs.add("-classpath");
    vargs.add(classPath.toString());
    
    // append class name and args
    vargs.add(TestGoraStorage.class.getName());
    vargs.add(String.valueOf(id));
    vargs.add(String.valueOf(start));
    vargs.add(String.valueOf(count));
    ProcessBuilder builder = new ProcessBuilder(vargs);
    return builder.start();
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: TestGoraStore <id> <startKey> <numRecords>");
      System.exit(-1);
    }
    TestGoraStorage test = new TestGoraStorage();
    test.init();
    int id = Integer.parseInt(args[0]);
    int start = Integer.parseInt(args[1]);
    int count = Integer.parseInt(args[2]);
    Worker w = test.new Worker(id, start, count, true);
    w.run();
    System.exit(0);
  }
}
