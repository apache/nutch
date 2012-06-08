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
package org.apache.nutch.api;

import java.util.List;
import java.util.logging.Level;

import org.apache.nutch.api.JobStatus.State;
import org.restlet.Component;
import org.restlet.data.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NutchServer {
  private static final Logger LOG = LoggerFactory.getLogger(NutchServer.class);
  
  private Component component;
  private NutchApp app;
  private int port;
  private boolean running;
  
  public NutchServer(int port) {
    this.port = port;
    // Create a new Component. 
    component = new Component();
    //component.getLogger().setLevel(Level.FINEST);
   
    // Add a new HTTP server listening on port 8182. 
    component.getServers().add(Protocol.HTTP, port); 
   
    // Attach the application. 
    app = new NutchApp();
    component.getDefaultHost().attach("/nutch", app);
    NutchApp.server = this;
  }
  
  public boolean isRunning() {
    return running;
  }
  
  public void start() throws Exception {
    LOG.info("Starting NutchServer on port " + port + "...");
    component.start();
    LOG.info("Started NutchServer on port " + port);
    running = true;
    NutchApp.started = System.currentTimeMillis();
  }
  
  public boolean canStop() throws Exception {
    List<JobStatus> jobs = NutchApp.jobMgr.list(null, State.RUNNING);
    if (!jobs.isEmpty()) {
      return false;
    }
    return true;
  }
  
  public boolean stop(boolean force) throws Exception {
    if (!running) {
      return true;
    }
    if (!canStop() && !force) {
      LOG.warn("Running jobs - can't stop now.");
      return false;
    }
    LOG.info("Stopping NutchServer on port " + port + "...");
    component.stop();
    LOG.info("Stopped NutchServer on port " + port);
    running = false;
    return true;
  }

  public static void main(String[] args) throws Exception { 
    if (args.length == 0) {
      System.err.println("Usage: NutchServer <port>");
      System.exit(-1);
    }
    int port = Integer.parseInt(args[0]);
    NutchServer server = new NutchServer(port);
    server.start();
  }
}
