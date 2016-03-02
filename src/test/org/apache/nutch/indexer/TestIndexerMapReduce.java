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
package org.apache.nutch.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.indexer.IndexerMapReduce.IndexerMapReduceMapper;
import org.apache.nutch.indexer.IndexerMapReduce.IndexerMapReduceReducer;
import org.apache.nutch.metadata.HttpHeaders;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the MapReduce functionality of the 
 * {@link org.apache.nutch.indexer.IndexingJob} by asserting data
 * at various stages of {@link org.apache.nutch.indexer.IndexerMapReduce}.
 * Specifically, we test data at various stages in both the
 * {@link org.apache.nutch.indexer.IndexerMapReduce.IndexerMapReduceMapper}
 * and {@link org.apache.nutch.indexer.IndexerMapReduce.IndexerMapReduceReducer}.
 * Finally, we test the entire MapReduce workflow asserting that the input
 * key and value map and reduce to an expected result. 
 */
public class TestIndexerMapReduce {

  private static Text key = new Text("http://www.thethrillseekers.co.uk/");

  MapDriver<Text, Writable, Text, NutchWritable> mapDriver;
  ReduceDriver<Text, NutchWritable, Text, NutchIndexAction> reduceDriver;
  MapReduceDriver<Text, Writable, Text, NutchWritable, Text, NutchIndexAction> mapReduceDriver;

  @Before
  public void setUp() {
    IndexerMapReduceMapper mapper = new IndexerMapReduceMapper();
    IndexerMapReduceReducer reducer = new IndexerMapReduceReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMapper() throws IOException {
    Writable crawlDatum = TestIndexerMapReduce.createCrawlDatum();
    mapDriver.withInput(key, crawlDatum);
    mapDriver.withOutput(key, new NutchWritable(crawlDatum)).run();
    //Assert.assertTrue(crawlDatum.compareTo(that));
  }

  @Test
  public void testReducer() throws IOException {
    List<NutchWritable> crawlDatums = TestIndexerMapReduce.createCrawlDatums();
    reduceDriver.withInput(key, crawlDatums);
    reduceDriver.withOutput(key, createNutchIndexAction()).run();
  }

  @Test
  public void testMapReduce() throws IOException {
    mapReduceDriver.withInput(key, TestIndexerMapReduce.createCrawlDatum());
    mapReduceDriver.withOutput(key, createNutchIndexAction()).run();
  }
  
  private static List<NutchWritable> createCrawlDatums() {
    List<NutchWritable> crawlDatums = new ArrayList<NutchWritable>();
    Writable crawlDatum1 = TestIndexerMapReduce.createCrawlDatum();
    Writable crawlDatum2 = TestIndexerMapReduce.createCrawlDatum();
    Writable crawlDatum3 = TestIndexerMapReduce.createCrawlDatum();
    crawlDatums.add(new NutchWritable(crawlDatum1));
    crawlDatums.add(new NutchWritable(crawlDatum2));
    crawlDatums.add(new NutchWritable(crawlDatum3));
    return crawlDatums;
  }

  private static Writable createCrawlDatum() {
    // create a date as of now, subtract 2 days from it for modified time
    Date date = new Date();
    CrawlDatum crawlDatum = new CrawlDatum();
    crawlDatum.setFetchInterval(0);
    crawlDatum.setFetchTime(date.getTime());
    // create metadata
    org.apache.hadoop.io.MapWritable md = new org.apache.hadoop.io.MapWritable();
    md.put(HttpHeaders.WRITABLE_CONTENT_TYPE, new Text(
        "text/html; charset=utf-8"));
    md.put(new Text("music_genre"), new Text("trance"));
    crawlDatum.setMetaData(md);
    Calendar calendar = Calendar.getInstance(Locale.getDefault());
    calendar.setTime(date);
    calendar.add(Calendar.DATE, -2);
    date.setTime(calendar.getTime().getTime());
    crawlDatum.setModifiedTime(date.getTime());
    crawlDatum.setRetriesSinceFetch(0);
    crawlDatum.setScore((float)0.0);
    crawlDatum.setSignature("4857485dd3a440180b1cc54b6b226b0b".getBytes());
    crawlDatum.setStatus(65);
    return crawlDatum;
  }

  private static NutchIndexAction createNutchIndexAction() {
    NutchDocument nDoc = new NutchDocument();
    NutchIndexAction nAction = new NutchIndexAction(nDoc, NutchIndexAction.ADD);
    return nAction;
  }

}
