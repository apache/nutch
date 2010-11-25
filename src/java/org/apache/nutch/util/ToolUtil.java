package org.apache.nutch.util;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.nutch.metadata.Nutch;

public class ToolUtil {

  public static final Map<String,Object> toArgMap(Object... args) {
    if (args == null) {
      return null;
    }
    if (args.length % 2 != 0) {
      throw new RuntimeException("expected pairs of argName argValue");
    }
    HashMap<String,Object> res = new HashMap<String,Object>();
    for (int i = 0; i < args.length; i += 2) {
      if (args[i + 1] != null) {
        res.put(String.valueOf(args[i]), args[i + 1]);
      }
    }
    return res;
  }
  
  public static final void recordJobStatus(String label, Job job, Map<String,Object> results) {
    Map<String,Object> jobs = (Map<String,Object>)results.get(Nutch.STAT_JOBS);
    if (jobs == null) {
      jobs = new LinkedHashMap<String,Object>();
      results.put(Nutch.STAT_JOBS, jobs);
    }
    Map<String,Object> stats = new HashMap<String,Object>();
    Map<String,Object> countStats = new HashMap<String,Object>();
    try {
      Counters counters = job.getCounters();
      for (CounterGroup cg : counters) {
        Map<String,Object> cnts = new HashMap<String,Object>();
        countStats.put(cg.getDisplayName(), cnts);
        for (Counter c : cg) {
          cnts.put(c.getName(), c.getValue());
        }
      }
    } catch (Exception e) {
      countStats.put("error", e.toString());
    }
    stats.put(Nutch.STAT_COUNTERS, countStats);
    stats.put("jobName", job.getJobName());
    stats.put("jobID", job.getJobID());
    if (label == null) {
      label = job.getJobName();
      if (job.getJobID() != null) {
        label = label + "-" + job.getJobID();
      }
    }
    jobs.put(label, stats);
  }
}
