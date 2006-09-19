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
