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
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;


import org.apache.nutch.api.model.response.NutchStatus;
import org.apache.nutch.api.model.response.JobInfo.State;
import org.apache.nutch.api.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path(value = "/admin")
public class AdminResource extends AbstractResource {
  private static final int DELAY_SEC = 10;
  private static final long DELAY_MILLIS = TimeUnit.SECONDS.toMillis(DELAY_SEC);

  private static final Logger LOG = LoggerFactory
      .getLogger(AdminResource.class);

  @Context
  SecurityContext securityContext;

  @GET
  @Path("/")
  public NutchStatus getNutchStatus(@Context HttpHeaders headers) {
    SecurityUtil.allowOnlyAdmin(securityContext);
    NutchStatus status = new NutchStatus();
    status.setStartDate(new Date(server.getStarted()));
    status.setConfiguration(configManager.list());
    status.setJobs(jobManager.list(null, State.ANY));
    status.setRunningJobs(jobManager.list(null, State.RUNNING));
    status.setActiveConfId(activeConfId);

    return status;
  }

  @GET
  @Path("/stop")
  @Produces(MediaType.TEXT_PLAIN)
  public String stop(@QueryParam("force") boolean force) {
    SecurityUtil.allowOnlyAdmin(securityContext);
    if (!server.canStop(force)) {
      LOG.info("Command 'stop' denied due to unfinished jobs");
      return "Can't stop now. There are jobs running. Try force option.";
    }

    scheduleServerStop();
    return new MessageFormat("Stopping in {0} seconds.", Locale.ROOT).format(DELAY_SEC);
  }

  private void scheduleServerStop() {
    SecurityUtil.allowOnlyAdmin(securityContext);
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
