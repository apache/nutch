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

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.nutch.api.ConfManager;
import org.apache.nutch.api.JobManager;
import org.apache.nutch.api.NutchServer;
import org.restlet.Context;

/**
 * Abstract base class for {@link NutchServer} REST APIs.
 */
@Produces({ MediaType.APPLICATION_JSON })
public abstract class AbstractResource {

  protected ConfManager configManager;
  protected JobManager jobManager;
  protected String activeConfId;
  protected NutchServer server;

  /**
   * Constructor method for {@link AbstractResource}
   * Retrieves {@link org.apache.nutch.api.NutchServer} information from {@link org.restlet.Context}
   */
  public AbstractResource() {
    server = (NutchServer) Context.getCurrent().getAttributes()
        .get(NutchServer.NUTCH_SERVER);
    configManager = server.getConfMgr();
    jobManager = server.getJobMgr();
    activeConfId = server.getActiveConfId();
  }

  /**
   * Throws HTTP 400 Bad Request Exception with given message
   *
   * @param message message to be placed at exception
   */
  protected void throwBadRequestException(String message) {
    throw new WebApplicationException(Response.status(Status.BAD_REQUEST)
        .entity(message).build());
  }
}
