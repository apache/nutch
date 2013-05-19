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
package org.apache.nutch.crawl;

import java.net.MalformedURLException;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.nutch.crawl.GeneratorJob.SelectorEntry;
import org.apache.nutch.crawl.URLPartitioner.FetchEntryPartitioner;
import org.apache.nutch.crawl.URLPartitioner.SelectorEntryPartitioner;
import org.apache.nutch.fetcher.FetchEntry;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;

/**
 * Tests {@link URLPartitioner}
 */
public class TestURLPartitioner {

  /**
   * tests one reducer, everything goes into one partition, using host partitioner.
   */
  @Test
  public void testOneReducer() {
    URLPartitioner partitioner = new URLPartitioner();
    Configuration conf = NutchConfiguration.create();
    conf.set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_HOST);
    partitioner.setConf(conf);
    
    int numReduceTasks = 1;
    
    assertEquals(0, partitioner.getPartition("http://example.org", numReduceTasks));
    assertEquals(0, partitioner.getPartition("http://www.apache.org", numReduceTasks)); 
  }
  
  /**
   * tests partitioning by host
   */
  @Test
  public void testModeHost() {
    URLPartitioner partitioner = new URLPartitioner();
    Configuration conf = NutchConfiguration.create();
    conf.set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_HOST);
    partitioner.setConf(conf);
    
    int numReduceTasks = 100;
    
    int partitionWithoutWWW = partitioner.getPartition("http://example.org/", numReduceTasks);
    int partitionWithWWW = partitioner.getPartition("http://www.example.org/", numReduceTasks);
    assertNotSame("partitions should differ because of different host", 
        partitionWithoutWWW, partitionWithWWW);
    
    int partitionSame1 = partitioner.getPartition("http://www.example.org/paris", numReduceTasks);
    int partitionSame2 = partitioner.getPartition("http://www.example.org/london", numReduceTasks);
    assertEquals("partitions should be same because of same host", 
        partitionSame1, partitionSame2);
  }
  
  /**
   * tests partitioning by domain
   */
  @Test
  public void testModeDomain() {
    URLPartitioner partitioner = new URLPartitioner();
    Configuration conf = NutchConfiguration.create();
    conf.set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_DOMAIN);
    partitioner.setConf(conf);
    
    int numReduceTasks = 100;
    
    int partitionExample = partitioner.getPartition("http://www.example.org/", numReduceTasks);
    int partitionApache = partitioner.getPartition("http://www.apache.org/", numReduceTasks);
    assertNotSame("partitions should differ because of different domain", 
        partitionExample, partitionApache);
    
    int partitionWithoutWWW = partitioner.getPartition("http://example.org/", numReduceTasks);
    int partitionWithWWW = partitioner.getPartition("http://www.example.org/", numReduceTasks);
    assertEquals("partitions should be same because of same domain", 
        partitionWithoutWWW, partitionWithWWW);
  }
  
  /**
   * tests partitioning by IP
   */
  @Test
  public void testModeIP() {
    URLPartitioner partitioner = new URLPartitioner();
    Configuration conf = NutchConfiguration.create();
    conf.set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_IP);
    partitioner.setConf(conf);
    
    int numReduceTasks = 100;
    
    int partitionExample = partitioner.getPartition("http://www.example.org/", numReduceTasks);
    int partitionApache = partitioner.getPartition("http://www.apache.org/", numReduceTasks);
    assertNotSame("partitions should differ because of different ip", 
        partitionExample, partitionApache);
    
    int partitionWithoutWWW = partitioner.getPartition("http://example.org/", numReduceTasks);
    int partitionWithWWW = partitioner.getPartition("http://www.example.org/", numReduceTasks);
    //the following has dependendy on example.org (that is has the same ip as www.example.org)
    assertEquals("partitions should be same because of same ip", 
        partitionWithoutWWW, partitionWithWWW);
  }
  
  /**
   * Test the seed functionality, using host partitioner.
   */
  @Test
  public void testSeed() {
    URLPartitioner partitioner = new URLPartitioner();
    Configuration conf = NutchConfiguration.create();
    conf.set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_HOST);
    partitioner.setConf(conf);
    
    int numReduceTasks = 100;
    int partitionNoSeed = partitioner.getPartition("http://example.org/", numReduceTasks);
    
    conf.setInt(URLPartitioner.PARTITION_URL_SEED, 1);
    partitioner.setConf(conf);
    
    int partitionWithSeed = partitioner.getPartition("http://example.org/", numReduceTasks);
    
    assertNotSame("partitions should differ because of different seed", 
        partitionNoSeed, partitionWithSeed);
  }

  
  /**
   * Tests the {@link SelectorEntryPartitioner}.
   */
  @Test
  public void testSelectorEntryPartitioner() {
    //The reference partitioner
    URLPartitioner refPartitioner = new URLPartitioner();
    
    //The to be tested partitioner with specific signature
    URLPartitioner.SelectorEntryPartitioner sigPartitioner = 
        new URLPartitioner.SelectorEntryPartitioner();
        
    Configuration conf = NutchConfiguration.create();
    conf.set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_HOST);
    
    refPartitioner.setConf(conf);
    sigPartitioner.setConf(conf);
    
    int numReduceTasks = 100;
    
    int partitionFromRef = refPartitioner.getPartition("http://www.example.org/", numReduceTasks);
    //init selector entry (score shouldn't matter)
    SelectorEntry selectorEntry = new SelectorEntry("http://www.example.org/", 1337);
    WebPage page = new WebPage();
    int partitionFromSig = sigPartitioner.getPartition(selectorEntry, page, numReduceTasks);
    
    assertEquals("partitions should be same", 
        partitionFromRef, partitionFromSig);
    
  }
  
  /**
   * Tests the {@link FetchEntryPartitioner}
   * @throws MalformedURLException 
   */
  @Test
  public void testFetchEntryPartitioner() throws MalformedURLException {
    //The reference partitioner
    URLPartitioner refPartitioner = new URLPartitioner();
    
    //The to be tested partitioner with specific signature
    URLPartitioner.FetchEntryPartitioner sigPartitioner = 
        new URLPartitioner.FetchEntryPartitioner();
        
    Configuration conf = NutchConfiguration.create();
    conf.set(URLPartitioner.PARTITION_MODE_KEY, URLPartitioner.PARTITION_MODE_HOST);
    
    refPartitioner.setConf(conf);
    sigPartitioner.setConf(conf);
    
    int numReduceTasks = 100;
    
    int partitionFromRef = refPartitioner.getPartition("http://www.example.org/", numReduceTasks);
    IntWritable intWritable = new IntWritable(1337); //doesn't matter
    WebPage page = new WebPage();
    String key = TableUtil.reverseUrl("http://www.example.org/");
    FetchEntry fetchEntry = new FetchEntry(conf, key, page);
    int partitionFromSig = sigPartitioner.getPartition(intWritable, fetchEntry, numReduceTasks);
    
    assertEquals("partitions should be same", 
        partitionFromRef, partitionFromSig);
    
  }
  
}
