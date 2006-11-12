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

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.Server;
import org.apache.nutch.searcher.DistributedSearch.Client;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class DistributedSearchTest extends TestCase {

  private static final int DEFAULT_PORT = 60000;
  private static final String DISTRIBUTED_SEARCH_TEST_PORT = "distributed.search.test.port";
  Configuration conf;
  Path searchdir=new Path("build/test/data/testcrawl/");
  Server server;
  
  protected void setUp() throws Exception {
    super.setUp();
    conf=NutchConfiguration.create();
    //set up server & start it
    server=DistributedSearch.Server.getServer(conf, searchdir, conf.getInt(DISTRIBUTED_SEARCH_TEST_PORT, DEFAULT_PORT));
    server.start();
  }

  protected void tearDown() throws Exception {
    super.tearDown();
    if(server!=null){
      //stop server
      //server.stop();
    }
  }

  public void testDistibutedSearch() throws IOException{

    int port=conf.getInt(DISTRIBUTED_SEARCH_TEST_PORT, DEFAULT_PORT);
    
    InetSocketAddress[] addresses=new InetSocketAddress[1];
    addresses[0]=new InetSocketAddress("localhost", port);
    
    Client c=new DistributedSearch.Client(addresses, conf);

    Query query=Query.parse("apache", conf);
    Hits hits=c.search(query, 5, null, null, false);
    c.getDetails(hits.getHit(0));
    assertTrue(hits.getTotal()>0);
  }
}
