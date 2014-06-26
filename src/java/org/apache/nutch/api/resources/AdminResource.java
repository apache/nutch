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
package org.apache.nutch.api.resources;

import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.apache.nutch.api.model.response.NutchStatus;
import org.apache.nutch.api.model.response.JobInfo.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path(value = "/admin")
public class AdminResource extends AbstractResource {
  private static final int DELAY_SEC = 10;
  private static final long DELAY_MILLIS = TimeUnit.SECONDS.toMillis(DELAY_SEC);

  private static final Logger LOG = LoggerFactory
      .getLogger(AdminResource.class);

  @GET
  @Path("/")
  public NutchStatus getNutchStatus() {
    NutchStatus status = new NutchStatus();

    status.setStartDate(new Date(server.getStarted()));
    status.setConfiguration(configManager.list());
    status.setJobs(jobManager.list(null, State.ANY));
    status.setRunningJobs(jobManager.list(null, State.RUNNING));

    return status;
  }

  @GET
  @Path("/stop")
  public String stop(@QueryParam("force") boolean force) {
    if (!server.canStop(force)) {
      LOG.info("Command 'stop' denied due to unfinished jobs");
      return "Can't stop now. There are jobs running. Try force option.";
    }

    scheduleServerStop();
    return MessageFormat.format("Stopping in {0} seconds.", DELAY_SEC);
  }

  private void scheduleServerStop() {
    LOG.info("Server shutdown scheduled in {} seconds", DELAY_SEC);
    Thread thread = new Thread() {
      public void run() {
        try {
          Thread.sleep(DELAY_MILLIS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        server.stop(false);
        LOG.info("Service stopped.");
      }
    };
    thread.setDaemon(true);
    thread.start();
    LOG.info("Service shutting down...");
  }

}
