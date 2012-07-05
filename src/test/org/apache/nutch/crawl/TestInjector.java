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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    if (fs.exists(urlPath)) fs.delete(urlPath, false);
    if (fs.exists(crawldbPath)) fs.delete(crawldbPath, true);
  }
  
  protected void tearDown() throws IOException{
    fs.delete(testdir, true);
  }

  public void testInject() throws IOException {
    ArrayList<String> urls=new ArrayList<String>();
    // We'll use a separate list for MD so we can still compare url with containsAll
    ArrayList<String> metadata=new ArrayList<String>();
    for(int i=0;i<100;i++) {
      urls.add("http://zzz.com/" + i + ".html");
      metadata.add("\tnutch.score=2." + i + "\tnutch.fetchInterval=171717\tkey=value");
    }
    CrawlDBTestUtil.generateSeedList(fs, urlPath, urls, metadata);
    
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
      // We'll overwrite previously injected records but preserve their original MD
      urls2.add("http://zzz.com/" + i + ".html");
    }
    CrawlDBTestUtil.generateSeedList(fs, urlPath, urls2);
    injector=new Injector(conf);
    conf.setBoolean("db.injector.update", true);
    injector.inject(crawldbPath, urlPath);
    urls.addAll(urls2);
    
    // verify results
    read=readCrawldb();

    Collections.sort(read);
    Collections.sort(urls);

    // We should have 100 less records because we've overwritten
    assertEquals(urls.size() - 100, read.size());
    
    assertTrue(read.containsAll(urls));
    assertTrue(urls.containsAll(read));
    
    // Check if we correctly preserved MD
    Map<String, CrawlDatum> records = readCrawldbRecords();
    
    // Iterate over the urls, we're looking for http://zzz.com/ prefixed URLs
    // so we can check for MD and score and interval
    Text writableKey = new Text("key");
    Text writableValue = new Text("value");
    for (String url : urls) {
      if (url.indexOf("http://zzz") == 0) {       
        // Check for fetch interval
        assertTrue(records.get(url).getFetchInterval() == 171717);
        // Check for default score
        assertTrue(records.get(url).getScore() != 1.0);
        // Check for MD key=value
        assertEquals(writableValue, records.get(url).getMetaData().get(writableKey));
      }
    }
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
  
  private HashMap<String,CrawlDatum> readCrawldbRecords() throws IOException{
    Path dbfile=new Path(crawldbPath,CrawlDb.CURRENT_NAME + "/part-00000/data");
    System.out.println("reading:" + dbfile);
    SequenceFile.Reader reader=new SequenceFile.Reader(fs, dbfile, conf);
    HashMap<String,CrawlDatum> read=new HashMap<String,CrawlDatum>();
    
    READ:
      do {
      Text key=new Text();
      CrawlDatum value=new CrawlDatum();
      if(!reader.next(key, value)) break READ;
      read.put(key.toString(), value);
    } while(true);

    return read;
  }
}
