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

import java.util.Map;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.nutch.api.JobStatus.State;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class AdminResource extends ServerResource {
  private static final int SHUTDOWN_DELAY = 1000;

  private static final Logger LOG = LoggerFactory.getLogger(AdminResource.class);

  public static final String PATH = "admin";
  public static final String DESCR = "Service admin actions";

  @Get("json")
  public Object execute() throws Exception {
    Map<String, Object> attributes = getRequestAttributes();
    String cmd = (String) attributes.get(Params.CMD);

    if (StringUtils.equalsIgnoreCase("status", cmd)) {
      return getNutchStatus();
    }
    
    if (StringUtils.equalsIgnoreCase("stop", cmd)) {
      boolean force = BooleanUtils.toBoolean(getQuery().getFirstValue(Params.FORCE));
      return stopServer(force);
    }
    return "Unknown command " + cmd;
  }

  private String stopServer(boolean force) throws Exception {
    if (!canStopServer(force)) {
      LOG.info("Command 'stop' denied due to unfinished jobs");
      return "can't stop now";
    }
    
    Thread t = new Thread() {
      public void run() {
        try {
          Thread.sleep(SHUTDOWN_DELAY);
          NutchApp.server.stop(false);
          LOG.info("Service stopped.");
        } catch (Exception e) {
          LOG.error("Error stopping", e);
        }
      }
    };
    t.setDaemon(true);
    t.start();
    LOG.info("Service shutting down...");
    return "stopping";
  }

  private boolean canStopServer(boolean force) throws Exception {
    return force || NutchApp.server.canStop();
  }

  private Map<String, Object> getNutchStatus() throws Exception {
    Map<String, Object> res = Maps.newHashMap();
    res.put("started", NutchApp.started);

    Map<String, Object> jobs = Maps.newHashMap();
    jobs.put("all", NutchApp.jobMgr.list(null, State.ANY));
    jobs.put("running", NutchApp.jobMgr.list(null, State.RUNNING));
    res.put("jobs", jobs);
    res.put("confs", NutchApp.confMgr.list());
    
    return res;
  }
}
