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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

/**
 * Basic injector test:
 * 1. Creates a text file with urls
 * 2. Injects them into crawldb
 * 3. Reads crawldb entries and verifies contents
 * 4. Injects more urls into webdb
 * 5. Reads crawldb entries and verifies contents
 * 
 * @author nutch-dev <nutch-dev at lucene.apache.org>
 */
public class TestInjector extends TestCase {

  private Configuration conf;
  private FileSystem fs;
  final static Path testdir=new Path("build/test/inject-test");
  Path crawldbPath;
  Path urlPath;
  
  protected void setUp() throws Exception {
    conf = CrawlDBTestUtil.createConfiguration();
    urlPath=new Path(testdir,"urls");
    crawldbPath=new Path(testdir,"crawldb");
    fs=FileSystem.get(conf);
    if (fs.exists(urlPath)) fs.delete(urlPath);
    if (fs.exists(crawldbPath)) fs.delete(crawldbPath);
  }
  
  protected void tearDown() throws IOException{
    fs.delete(testdir);
  }

  public void testInject() throws IOException {
    ArrayList<String> urls=new ArrayList<String>();
    for(int i=0;i<100;i++) {
      urls.add("http://zzz.com/" + i + ".html");
    }
    CrawlDBTestUtil.generateSeedList(fs, urlPath, urls);
    
    Injector injector=new Injector(conf);
    injector.inject(crawldbPath, urlPath);
    
    // verify results
    List<String>read=readCrawldb();
    
    Collections.sort(read);
    Collections.sort(urls);

    assertEquals(urls.size(), read.size());
    
    assertTrue(read.containsAll(urls));
    assertTrue(urls.containsAll(read));
    
    //inject more urls
    ArrayList<String> urls2=new ArrayList<String>();
    for(int i=0;i<100;i++) {
      urls2.add("http://xxx.com/" + i + ".html");
    }
    CrawlDBTestUtil.generateSeedList(fs, urlPath, urls2);
    injector.inject(crawldbPath, urlPath);
    urls.addAll(urls2);
    
    // verify results
    read=readCrawldb();
    

    Collections.sort(read);
    Collections.sort(urls);

    assertEquals(urls.size(), read.size());
    
    assertTrue(read.containsAll(urls));
    assertTrue(urls.containsAll(read));
    
  }
  
  private List<String> readCrawldb() throws IOException{
    Path dbfile=new Path(crawldbPath,CrawlDb.CURRENT_NAME + "/part-00000/data");
    System.out.println("reading:" + dbfile);
    SequenceFile.Reader reader=new SequenceFile.Reader(fs, dbfile, conf);
    ArrayList<String> read=new ArrayList<String>();
    
    READ:
      do {
      Text key=new Text();
      CrawlDatum value=new CrawlDatum();
      if(!reader.next(key, value)) break READ;
      read.add(key.toString());
    } while(true);

    return read;
  }

}
