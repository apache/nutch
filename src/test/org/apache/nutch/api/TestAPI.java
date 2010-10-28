package org.apache.nutch.api;

import java.util.HashMap;
import java.util.Map;

import org.apache.nutch.api.JobManager.JobType;
import org.restlet.ext.jackson.JacksonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.ClientResource;

import junit.framework.TestCase;

public class TestAPI extends TestCase {
  
  NutchServer server;
  ClientResource cli;
  
  String baseUrl = "http://localhost:8192/nutch/";
  
  public void setUp() throws Exception {
    server = new NutchServer(8192);
    server.start();
  }
  
  public void tearDown() throws Exception {
    if (!server.stop(false)) {
      for (int i = 1; i < 11; i++) {
        System.err.println("Waiting for jobs to complete - " + i + "s");
        try {
          Thread.sleep(1000);
        } catch (Exception e) {};
        server.stop(false);
        if (!server.isRunning()) {
          break;
        }
      }
    }
    if (server.isRunning()) {
      System.err.println("Forcibly stopping server...");
      server.stop(true);
    }
  }
  
  public void testInfoAPI() throws Exception {
    ClientResource cli = new ClientResource(baseUrl);
    String expected = "[[\"confs\",\"Configuration manager\"],[\"jobs\",\"Job manager\"]]";
    String got = cli.get().getText();
    assertEquals(expected, got);
  }
  
  public void testConfsAPI() throws Exception {
    ClientResource cli = new ClientResource(baseUrl + ConfResource.PATH);
    assertEquals("[\"default\"]", cli.get().getText());
    // create
    Map<String,Object> map = new HashMap<String,Object>();
    map.put(Params.CONF_ID, "test");
    HashMap<String,String> props = new HashMap<String,String>();
    props.put("testProp", "blurfl");
    map.put(Params.PROPS, props);
    JacksonRepresentation<Map<String,Object>> jr =
      new JacksonRepresentation<Map<String,Object>>(map);
    System.out.println(cli.put(jr).getText());
    assertEquals("[\"default\",\"test\"]", cli.get().getText());
    cli = new ClientResource(baseUrl + ConfResource.PATH + "/test");
    Map res = cli.get(Map.class);
    assertEquals("blurfl", res.get("testProp"));
    // delete
    cli.delete();
    cli = new ClientResource(baseUrl + ConfResource.PATH);
    assertEquals("[\"default\"]", cli.get().getText());
  }
  
  public void testJobsAPI() throws Exception {
    ClientResource cli = new ClientResource(baseUrl + JobResource.PATH);
    assertEquals("[]", cli.get().getText());
    // create
    Map<String,Object> map = new HashMap<String,Object>();
    map.put(Params.JOB_TYPE, JobType.READDB.toString());
    map.put(Params.CONF_ID, "default");
    JacksonRepresentation<Map<String,Object>> jr =
      new JacksonRepresentation<Map<String,Object>>(map);
    Representation r = cli.put(map);
    String jobId = r.getText();
    assertNotNull(jobId);
    assertTrue(jobId.startsWith("default-READDB-"));
    // list
    Map[] list = cli.get(Map[].class);
    assertEquals(1, list.length);
    String id = (String)list[0].get("id");
    String state = (String)list[0].get("state");
    assertEquals(jobId, id);
    assertEquals(state, "RUNNING");
  }
}
