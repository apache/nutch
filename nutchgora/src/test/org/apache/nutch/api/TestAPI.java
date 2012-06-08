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

// CURRENTLY DISABLED. TESTS ARE FLAPPING FOR NO APPARENT REASON.
// SHALL BE FIXED OR REPLACES BY NEW API IMPLEMENTATION

package org.apache.nutch.api;

//import static org.junit.Assert.*;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.nutch.api.JobManager.JobType;
//import org.apache.nutch.metadata.Nutch;
//import org.apache.nutch.util.NutchTool;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
//import org.restlet.ext.jackson.JacksonRepresentation;
//import org.restlet.representation.Representation;
//import org.restlet.resource.ClientResource;

public class TestAPI {
  @Test
  public void test() throws Exception {}
//  
//  private static NutchServer server;
//  ClientResource cli;
//  
//  private static String baseUrl = "http://localhost:8192/nutch/";
//  
//  @BeforeClass
//  public static void before() throws Exception {
//    server = new NutchServer(8192);
//    server.start();
//  }
//  
//  @AfterClass
//  public static void after() throws Exception {
//    if (!server.stop(false)) {
//      for (int i = 1; i < 11; i++) {
//        System.err.println("Waiting for jobs to complete - " + i + "s");
//        try {
//          Thread.sleep(1000);
//        } catch (Exception e) {};
//        server.stop(false);
//        if (!server.isRunning()) {
//          break;
//        }
//      }
//    }
//    if (server.isRunning()) {
//      System.err.println("Forcibly stopping server...");
//      server.stop(true);
//    }
//  }
//  
//  @Test
//  public void testInfoAPI() throws Exception {
//    ClientResource cli = new ClientResource(baseUrl);
//    String expected = "[[\"admin\",\"Service admin actions\"],[\"confs\",\"Configuration manager\"],[\"db\",\"DB data streaming\"],[\"jobs\",\"Job manager\"]]";
//    String got = cli.get().getText();
//    assertEquals(expected, got);
//  }
//  
//  @SuppressWarnings("rawtypes")
//  @Test
//  public void testConfsAPI() throws Exception {
//    ClientResource cli = new ClientResource(baseUrl + ConfResource.PATH);
//    assertEquals("[\"default\"]", cli.get().getText());
//    // create
//    Map<String,Object> map = new HashMap<String,Object>();
//    map.put(Params.CONF_ID, "test");
//    HashMap<String,String> props = new HashMap<String,String>();
//    props.put("testProp", "blurfl");
//    map.put(Params.PROPS, props);
//    JacksonRepresentation<Map<String,Object>> jr =
//      new JacksonRepresentation<Map<String,Object>>(map);
//    System.out.println(cli.put(jr).getText());
//    assertEquals("[\"default\",\"test\"]", cli.get().getText());
//    cli = new ClientResource(baseUrl + ConfResource.PATH + "/test");
//    Map res = cli.get(Map.class);
//    assertEquals("blurfl", res.get("testProp"));
//    // delete
//    cli.delete();
//    cli = new ClientResource(baseUrl + ConfResource.PATH);
//    assertEquals("[\"default\"]", cli.get().getText());
//  }
//  
//  @SuppressWarnings("rawtypes")
//  @Test
//  public void testJobsAPI() throws Exception {
//    ClientResource cli = new ClientResource(baseUrl + JobResource.PATH);
//    assertEquals("[]", cli.get().getText());
//    // create
//    Map<String,Object> map = new HashMap<String,Object>();
//    map.put(Params.JOB_TYPE, JobType.READDB.toString());
//    map.put(Params.CONF_ID, "default");
//    Representation r = cli.put(map);
//    String jobId = r.getText();
//    assertNotNull(jobId);
//    assertTrue(jobId.startsWith("default-READDB-"));
//    // list
//    Map[] list = cli.get(Map[].class);
//    assertEquals(1, list.length);
//    String id = (String)list[0].get("id");
//    String state = (String)list[0].get("state");
//    assertEquals(jobId, id);
//    assertEquals(state, "RUNNING");
//    int cnt = 10;
//    do {
//      try {
//        Thread.sleep(2000);
//      } catch (Exception e) {};
//      list = cli.get(Map[].class);
//      state = (String)list[0].get("state");
//      if (!state.equals("RUNNING")) {
//        break;
//      }
//    } while (--cnt > 0);
//    assertTrue(cnt > 0);
//    if (list == null) return;
//    for (Map m : list) {
//      System.out.println(m);
//    }
//  }
//  
//  @SuppressWarnings("unchecked")
//  @Test
//  public void testStopKill() throws Exception {
//    ClientResource cli = new ClientResource(baseUrl + JobResource.PATH);
//    // create
//    Map<String,Object> map = new HashMap<String,Object>();
//    map.put(Params.JOB_TYPE, JobType.CLASS.toString());
//    Map<String,Object> args = new HashMap<String,Object>();
//    map.put(Params.ARGS, args);
//    args.put(Nutch.ARG_CLASS, SpinningJob.class.getName());
//    map.put(Params.CONF_ID, "default");
//    Representation r = cli.put(map);
//    String jobId = r.getText();
//    cli.release();
//    assertNotNull(jobId);
//    System.out.println(jobId);
//    assertTrue(jobId.startsWith("default-CLASS-"));
//    ClientResource stopCli = new ClientResource(baseUrl + JobResource.PATH +
//        "?job=" + jobId + "&cmd=stop");
//    r = stopCli.get();
//    assertEquals("true", r.getText());
//    stopCli.release();
//    Thread.sleep(2000); // wait for the job to finish
//    ClientResource jobCli = new ClientResource(baseUrl + JobResource.PATH + "/" + jobId);
//    Map<String,Object> res = jobCli.get(Map.class);
//    res = (Map<String,Object>)res.get("result");
//    assertEquals("stopped", res.get("res"));
//    jobCli.release();
//    // restart and kill
//    r = cli.put(map);
//    jobId = r.getText();
//    cli.release();
//    assertNotNull(jobId);
//    System.out.println(jobId);
//    assertTrue(jobId.startsWith("default-CLASS-"));
//    ClientResource killCli = new ClientResource(baseUrl + JobResource.PATH +
//        "?job=" + jobId + "&cmd=abort");
//    r = killCli.get();
//    assertEquals("true", r.getText());
//    killCli.release();
//    Thread.sleep(2000); // wait for the job to finish
//    jobCli = new ClientResource(baseUrl + JobResource.PATH + "/" + jobId);
//    res = jobCli.get(Map.class);
//    res = (Map<String,Object>)res.get("result");
//    assertEquals("killed", res.get("res"));
//    jobCli.release();
//  }
//  
//  public static class SpinningJob extends NutchTool {
//    volatile boolean shouldStop = false;
//
//    @Override
//    public Map<String, Object> run(Map<String, Object> args) throws Exception {
//      status.put(Nutch.STAT_MESSAGE, "running");
//      int cnt = 60;
//      while (!shouldStop && cnt-- > 0) {
//        Thread.sleep(1000);
//      }
//      if (cnt == 0) {
//        results.put("res", "failed");
//      }
//      return results;
//    }
//
//    @Override
//    public boolean stopJob() throws Exception {
//      results.put("res", "stopped");
//      shouldStop = true;
//      return true;
//    }
//
//    @Override
//    public boolean killJob() throws Exception {
//      results.put("res", "killed");
//      shouldStop = true;
//      return true;
//    }
//    
//  }
}
