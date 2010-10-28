package org.apache.nutch.api;

import java.util.Map;

import org.apache.nutch.api.JobManager.JobType;

public class JobStatus {
  public static enum State {IDLE, RUNNING, FINISHED, FAILED, KILLED, ANY};
  public String id;
  public JobType type;
  public String confId;
  public Object[] args;
  public Map<String,Object> result;
  public State state;
  public String msg;
  
  public JobStatus(String id, JobType type, String confId, Object[] args,
      State state, String msg) {
    this.id = id;
    this.type = type;
    this.confId = confId;
    this.args = args;
    this.state = state;
    this.msg = msg;
  }

}
