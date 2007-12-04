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
package org.apache.nutch.searcher;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.Server;
import org.apache.nutch.searcher.DistributedSearch.Client;
import org.apache.nutch.util.NutchConfiguration;

public class TestDistributedSearch
  extends TestCase {

  private static final int DEFAULT_PORT = 60000;
  private static final String DISTRIBUTED_SEARCH_TEST_PORT = "distributed.search.test.port";

  private static final int DEFAULT_PORT1 = 60001;
  private static final String DISTRIBUTED_SEARCH_TEST_PORT1 = "distributed.search.test.port1";
  
  private static final int DEFAULT_PORT2 = 60002;
  private static final String DISTRIBUTED_SEARCH_TEST_PORT2 = "distributed.search.test.port2";
  
  Path searchdir = new Path("build/test/data/testcrawl/");

  public void testDistibutedSearch() 
    throws IOException {
  
    Configuration conf = NutchConfiguration.create();
    
    //set up server & start it
    Server server = DistributedSearch.Server.getServer(conf, searchdir, 
      conf.getInt(DISTRIBUTED_SEARCH_TEST_PORT, DEFAULT_PORT));
    server.start();
    
    int port = conf.getInt(DISTRIBUTED_SEARCH_TEST_PORT, DEFAULT_PORT);
    
    InetSocketAddress[] addresses = new InetSocketAddress[1];
    addresses[0] = new InetSocketAddress("localhost", port);
    
    Client c = new DistributedSearch.Client(addresses, conf);
  
    Query query = Query.parse("apache", conf);
    Hits hits = c.search(query, 5, null, null, false);
    c.getDetails(hits.getHit(0));
    assertTrue(hits.getTotal() > 0);
    
    if(server != null){
      server.stop();
    }
  }
  
  public void testUpdateSegments() 
    throws IOException {
    
    // Startup 2 search servers. One was already started in setup, start another 
    // one at a different port
    
    Configuration conf = NutchConfiguration.create();
    
    Server server1 = DistributedSearch.Server.getServer(conf, searchdir, 
      conf.getInt(DISTRIBUTED_SEARCH_TEST_PORT1, DEFAULT_PORT1));
    
    Server server2 = DistributedSearch.Server.getServer(conf, searchdir, 
      conf.getInt(DISTRIBUTED_SEARCH_TEST_PORT2, DEFAULT_PORT2));
    
    server1.start();
    server2.start();
    
    /* create a new file search-servers.txt
     * with 1 server at port 60000
     */
    FileSystem fs = FileSystem.get(conf);
    Path testServersPath = new Path(searchdir, "search-server.txt");
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(
      testServersPath, true)));    
    bw.write("localhost " + DEFAULT_PORT1 + "\n");    
    bw.flush();
    bw.close();
  
    /* 
     * Check if it found the server
     */
    Client c = new DistributedSearch.Client(testServersPath, conf);
    boolean[] liveServers = c.getLiveServer();
    assertEquals(liveServers.length, 1);
  
    /* Add both the servers at ports 60000 & 60005 
     * to the search-server.txt file
     */
    
    // give the servers a little time to wait for file modification
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    bw = new BufferedWriter(new OutputStreamWriter(fs.create(testServersPath,
      true)));    
    bw.write("localhost " + DEFAULT_PORT1 + "\n");    
    bw.write("localhost " + DEFAULT_PORT2 + "\n");
    bw.flush();
    bw.close();
    
    // Check if it found both the servers
    c.updateSegments();
    

    liveServers = c.getLiveServer();
    assertEquals(liveServers.length, 2);
  
    if (server1 != null) {
      server1.stop();
    }
    if (server2 != null) {
      server2.stop();
    }
    
    fs.delete(testServersPath);    
  }
}
