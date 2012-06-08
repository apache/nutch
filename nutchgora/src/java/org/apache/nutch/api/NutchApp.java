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

import org.apache.nutch.api.impl.RAMConfManager;
import org.apache.nutch.api.impl.RAMJobManager;
import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.routing.Router;

public class NutchApp extends Application {
  public static ConfManager confMgr;
  public static JobManager jobMgr;
  public static NutchServer server;
  public static long started;
  
  static {
    confMgr = new RAMConfManager();
    jobMgr = new RAMJobManager();
  }
  
  /**
   * Creates a root Restlet that will receive all incoming calls.
   */
  @Override
  public synchronized Restlet createInboundRoot() {
      getTunnelService().setEnabled(true);
      getTunnelService().setExtensionsTunnel(true);
      Router router = new Router(getContext());
      //router.getLogger().setLevel(Level.FINEST);
      // configs
      router.attach("/", APIInfoResource.class);
      router.attach("/" + AdminResource.PATH, AdminResource.class);
      router.attach("/" + AdminResource.PATH + "/{" + Params.CMD + 
          "}", AdminResource.class);
      router.attach("/" + ConfResource.PATH, ConfResource.class);
      router.attach("/" + ConfResource.PATH + "/{"+ Params.CONF_ID +
          "}", ConfResource.class);
      router.attach("/" + ConfResource.PATH + "/{" + Params.CONF_ID +
          "}/{" + Params.PROP_NAME + "}", ConfResource.class);
      // db
      router.attach("/" + DbResource.PATH, DbResource.class);
      // jobs
      router.attach("/" + JobResource.PATH, JobResource.class);
      router.attach("/" + JobResource.PATH + "/{" + Params.JOB_ID + "}",
          JobResource.class);
      router.attach("/" + JobResource.PATH, JobResource.class);
      router.attach("/" + JobResource.PATH + "/{" + Params.JOB_ID + "}/{" +
          Params.CMD + "}", JobResource.class);
      return router;
  }
}
