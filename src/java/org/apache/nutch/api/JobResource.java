package org.apache.nutch.api;

import java.util.Map;

import org.apache.nutch.api.JobManager.JobType;
import org.apache.nutch.api.JobStatus.State;
import org.restlet.resource.Get;
import org.restlet.resource.Put;
import org.restlet.resource.ServerResource;

public class JobResource extends ServerResource {
  public static final String PATH = "jobs";
  public static final String DESCR = "Job manager";
  
  @Get("json")
  public Object retrieve() throws Exception {
    String cid = (String)getRequestAttributes().get(Params.CRAWL_ID);
    String jid = (String)getRequestAttributes().get(Params.JOB_ID);
    if (jid == null) {
      return NutchApp.jobMgr.list(cid, State.ANY);
    } else {
      // handle stop / abort / get
      String cmd = (String)getRequestAttributes().get(Params.CMD);      
      if (cmd == null) {
        return NutchApp.jobMgr.get(cid, jid);
      }
      if (cmd.equals(Params.JOB_CMD_STOP)) {
        return NutchApp.jobMgr.abort(cid, jid);
      } else if (cmd.equals(Params.JOB_CMD_ABORT)) {
        return NutchApp.jobMgr.abort(cid, jid);
      } else if (cmd.equals(Params.JOB_CMD_GET)) {
        return NutchApp.jobMgr.get(cid, jid);
      } else {
        throw new Exception("Unknown command: " + cmd);
      }
    }
  }
  
  /*
   * String crawlId
   * String type
   * String confId
   * Object[] args
   */
  @Put("json")
  public Object create(Map<String,Object> args) throws Exception {
    String cid = (String)args.get(Params.CRAWL_ID);
    String typeString = (String)args.get(Params.JOB_TYPE);
    JobType type = JobType.valueOf(typeString);
    String confId = (String)args.get(Params.CONF_ID);
    Object[] cmdArgs = (Object[])args.get(Params.ARGS);
    String jobId = NutchApp.jobMgr.create(cid, type, confId, cmdArgs);
    return jobId;
  }
}
