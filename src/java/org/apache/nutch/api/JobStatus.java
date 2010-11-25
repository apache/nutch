package org.apache.nutch.api;

import java.util.Map;

import org.apache.nutch.api.JobManager.JobType;
import org.apache.nutch.util.NutchTool;

public class JobStatus {
  public static enum State {IDLE, RUNNING, FINISHED, FAILED, KILLED,
    STOPPING, KILLING, ANY};
  public String id;
  public JobType type;
  public String confId;
  public Map<String,Object> args;
  public Map<String,Object> result;
  public NutchTool tool;
  public State state;
  public String msg;
  
  public JobStatus(String id, JobType type, String confId, Map<String,Object> args,
      State state, String msg) {
    this.id = id;
    this.type = type;
    this.confId = confId;
    this.args = args;
    this.state = state;
    this.msg = msg;
  }

}
