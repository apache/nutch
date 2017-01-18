/**
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
package org.apache.nutch.service.resources;

import java.lang.invoke.MethodHandles;
import java.util.Date;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.apache.nutch.service.model.response.JobInfo.State;
import org.apache.nutch.service.model.response.NutchServerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path(value="/admin")
public class AdminResource extends AbstractResource{

  private final int DELAY_SEC = 1;
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /**
   * To get the status of the Nutch Server 
   * @return
   */
  @GET
  @Path(value="/")
  public NutchServerInfo getServerStatus(){
    NutchServerInfo serverInfo = new NutchServerInfo();
    serverInfo.setConfiguration(configManager.list());
    serverInfo.setStartDate(new Date(server.getStarted()));
    serverInfo.setJobs(jobManager.list(null, State.ANY));
    serverInfo.setRunningJobs(jobManager.list(null, State.RUNNING));    
    return serverInfo;
  }

  /**
   * Stop the Nutch server
   * @param force If set to true, it will kill any running jobs
   * @return
   */
  @GET
  @Path(value="/stop")
  public String stopServer(@QueryParam("force") boolean force){
    if(!server.canStop(force)){
      return "Jobs still running -- Cannot stop server now" ;
    }    
    scheduleServerStop();
    return "Stopping in server on port " + server.getPort();
  }

  private void scheduleServerStop() {
    LOG.info("Shutting down server in {} sec", DELAY_SEC);
    Thread thread = new Thread() {
      public void run() {
        try {
          Thread.sleep(DELAY_SEC*1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        server.stop();
        LOG.info("Service stopped.");
      }
    };
    thread.setDaemon(true);
    thread.start();
    LOG.info("Service shutting down...");
  }

}
