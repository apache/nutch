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

import java.util.HashMap;
import java.util.Map;

import org.apache.nutch.api.JobStatus.State;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminResource extends ServerResource {
  private static final Logger LOG = LoggerFactory.getLogger(AdminResource.class);

  public static final String PATH = "admin";
  public static final String DESCR = "Service admin actions";

  @Get("json")
  public Object execute() throws Exception {
    String cmd = (String)getRequestAttributes().get(Params.CMD);
    if ("status".equalsIgnoreCase(cmd)) {
      // status
      Map<String,Object> res = new HashMap<String,Object>();
      res.put("started", NutchApp.started);
      Map<String,Object> jobs = new HashMap<String,Object>();      
      jobs.put("all", NutchApp.jobMgr.list(null, State.ANY));
      jobs.put("running", NutchApp.jobMgr.list(null, State.RUNNING));
      res.put("jobs", jobs);
      res.put("confs", NutchApp.confMgr.list());
      return res;
    } else if ("stop".equalsIgnoreCase(cmd)) {
      // stop
      if (NutchApp.server.canStop()) {
        Thread t = new Thread() {
          public void run() {
            try {
              Thread.sleep(1000);
              NutchApp.server.stop(false);
              LOG.info("Service stopped.");
            } catch (Exception e) {
              LOG.error("Error stopping", e);
            };
          }
        };
        t.setDaemon(true);
        t.start();
        LOG.info("Service shutting down...");
        return "stopping";
      } else {
        LOG.info("Command 'stop' denied due to unfinished jobs");
        return "can't stop now";
      }
    } else {
      return "Unknown command " + cmd;
    }
  }
}
