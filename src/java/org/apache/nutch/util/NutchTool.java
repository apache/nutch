package org.apache.nutch.util;

import java.util.Map;

import org.apache.hadoop.mapreduce.Job;

public interface NutchTool {
  
  /** Prepares the tool. May return additional info, or null. */
  public Map<String,Object> prepare() throws Exception;

  /** Create jobs to be executed in sequence. */
  public Job[] createJobs(Object... args) throws Exception;
  
  /** Post-process results of a job. */
  public Map<String,Object> postJob(int jobIndex, Job job) throws Exception;
  
  /** Finish processing and optionally return results. */
  public Map<String,Object> finish() throws Exception;
}