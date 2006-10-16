/*
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDBTestUtil.URLCrawlDatum;

import junit.framework.TestCase;

/**
 * Basic generator test:
 * 1. Insert entries in crawldb
 * 2. Generates entries to fetch
 * 3. Verifies that number of generated urls match
 * 4. Verifies that highest scoring urls are generated 
 
 * @author nutch-dev <nutch-dev at lucene.apache.org>
 *
 */
public class TestGenerator extends TestCase {

  Configuration conf;

  Path dbDir;

  Path segmentsDir;

  FileSystem fs;

  final static Path testdir=new Path("build/test/generator-test");

  protected void setUp() throws Exception {
    conf = CrawlDBTestUtil.createConfiguration();
    fs=FileSystem.get(conf);
    fs.delete(testdir);
  }

  protected void tearDown() {
    delete(testdir);
  }

  private void delete(Path p) {
    try {
      fs.delete(p);
    } catch (IOException e) {
    }
  }

  /**
   * Test that generator generates fetchlish ordered by score (desc)
   * 
   * @throws Exception
   */
  public void testGenerateHighest() throws Exception {

    int NUM_RESULTS=2;
 
    ArrayList<URLCrawlDatum> list = new ArrayList<URLCrawlDatum>();
    
    for(int i=0;i<=100;i++){
      list.add(new CrawlDBTestUtil.URLCrawlDatum(new Text("http://aaa/" + pad(i)),
        new CrawlDatum(CrawlDatum.STATUS_DB_UNFETCHED, 1, i)));
    }
    
    dbDir = new Path(testdir, "crawldb");
    segmentsDir = new Path(testdir, "segments");
    fs.mkdirs(dbDir);
    fs.mkdirs(segmentsDir);
    
    // create crawldb
    CrawlDBTestUtil.createCrawlDb(fs, dbDir, list);
    
    // generate segment
    Generator g=new Generator(conf);
    Path generatedSegment=g.generate(dbDir, segmentsDir, -1, NUM_RESULTS, Long.MAX_VALUE);
    
    Path fetchlist=new Path(new Path(generatedSegment, CrawlDatum.GENERATE_DIR_NAME), "part-00000");
    
    // verify results
    SequenceFile.Reader reader=new SequenceFile.Reader(fs, fetchlist, conf);
    
    ArrayList<URLCrawlDatum> l=new ArrayList<URLCrawlDatum>();
    
    READ:
      do {
      Text key=new Text();
      CrawlDatum value=new CrawlDatum();
      if(!reader.next(key, value)) break READ;
      l.add(new URLCrawlDatum(key, value));
    } while(true);

    reader.close();

    // sort urls by score desc
    Collections.sort(l, new ScoreComparator());

    //verify we got right amount of records
    assertEquals(NUM_RESULTS, l.size());

    //verify we have the highest scoring urls
    assertEquals("http://aaa/100", (l.get(0).url.toString()));
    assertEquals("http://aaa/099", (l.get(1).url.toString()));
  }

  private String pad(int i) {
    String s=Integer.toString(i);
    while(s.length()<3)
      s="0" + s;
    return s;
  }

  /**
   * Comparator that sorts by score desc
   */
  public class ScoreComparator implements Comparator<URLCrawlDatum> {

    public int compare(URLCrawlDatum tuple1, URLCrawlDatum tuple2) {

      if (tuple2.datum.getScore() - tuple1.datum.getScore() < 0)
        return -1;
      if (tuple2.datum.getScore() - tuple1.datum.getScore() > 0)
        return 1;

      return 0;
    }
  }
}
