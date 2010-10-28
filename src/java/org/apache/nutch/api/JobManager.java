package org.apache.nutch.api;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nutch.api.JobStatus.State;

public interface JobManager {
  
  public static enum JobType {INJECT, GENERATE, FETCH, PARSE, UPDATEDB, INDEX, CRAWL, READDB, CLASS};

  public List<JobStatus> list(String crawlId, State state) throws Exception;
  
  public Map<String,String> get(String crawlId, String id) throws Exception;
  
  public String create(String crawlId, JobType type, String confId, Object... args) throws Exception;
  
  public boolean abort(String crawlId, String id) throws Exception;
  
  public boolean stop(String crawlId, String id) throws Exception;
}
