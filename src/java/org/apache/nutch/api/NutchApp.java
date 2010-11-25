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
