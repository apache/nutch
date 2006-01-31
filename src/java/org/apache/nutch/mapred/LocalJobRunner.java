/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.mapred;

import java.io.*;
import java.util.*;
import java.util.logging.*;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.util.*;

/** Implements MapReduce locally, in-process, for debugging. */ 
public class LocalJobRunner implements JobSubmissionProtocol {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.mapred.LocalJobRunner");

  private NutchFileSystem fs;
  private HashMap jobs = new HashMap();
  private NutchConf nutchConf;

  private class Job extends Thread
    implements TaskUmbilicalProtocol {
    private String file;
    private String id;
    private JobConf job;

    private JobStatus status = new JobStatus();
    private ArrayList mapIds = new ArrayList();
    private MapOutputFile mapoutputFile;

    public Job(String file, NutchConf nutchConf) throws IOException {
      this.file = file;
      this.id = "job_" + newId();
      this.mapoutputFile = new MapOutputFile();
      this.mapoutputFile.setConf(nutchConf);

      File localFile = new JobConf(nutchConf).getLocalFile("localRunner", id+".xml");
      fs.copyToLocalFile(new File(file), localFile);
      this.job = new JobConf(localFile);
      
      
      this.status.jobid = id;
      this.status.runState = JobStatus.RUNNING;

      jobs.put(id, this);

      this.start();
    }

    public void run() {
      try {
        // split input into minimum number of splits
        FileSplit[] splits = job.getInputFormat().getSplits(fs, job, 1);

        // run a map task for each split
        job.setNumReduceTasks(1);                 // force a single reduce task
        for (int i = 0; i < splits.length; i++) {
          mapIds.add("map_" + newId());
          MapTask map = new MapTask(file, (String)mapIds.get(i), splits[i]);
          map.setConf(job);
          map.run(job, this);
        }

        // move map output to reduce input
        String reduceId = "reduce_" + newId();
        for (int i = 0; i < mapIds.size(); i++) {
          String mapId = (String)mapIds.get(i);
          File mapOut = this.mapoutputFile.getOutputFile(mapId, 0);
          File reduceIn = this.mapoutputFile.getInputFile(mapId, reduceId);
          reduceIn.getParentFile().mkdirs();
          if (!NutchFileSystem.getNamed("local", this.job).rename(mapOut, reduceIn))
            throw new IOException("Couldn't rename " + mapOut);
          this.mapoutputFile.removeAll(mapId);
        }

        // run a single reduce task
        ReduceTask reduce =
          new ReduceTask(file, reduceId,
                         (String[])mapIds.toArray(new String[0]),
                         0);
        reduce.setConf(job);
        reduce.run(job, this);
        this.mapoutputFile.removeAll(reduceId);
        
        this.status.runState = JobStatus.SUCCEEDED;

      } catch (Throwable t) {
        this.status.runState = JobStatus.FAILED;
        t.printStackTrace();
      }
    }

    private String newId() {
      return Integer.toString(Math.abs(new Random().nextInt()),36);
    }

    // TaskUmbilicalProtocol methods

    public Task getTask(String taskid) { return null; }

    public void progress(String taskId, float progress, String state) {
      LOG.info(state);
      float taskIndex = mapIds.indexOf(taskId);
      if (taskIndex >= 0) {                       // mapping
        float numTasks = mapIds.size();
        status.mapProgress = (taskIndex/numTasks)+(progress/numTasks);
      } else {
        status.reduceProgress = progress;
      }
    }

    public void reportDiagnosticInfo(String taskid, String trace) {
      // Ignore for now
    }

    public void ping(String taskid) throws IOException {}

    public void done(String taskId) throws IOException {
      int taskIndex = mapIds.indexOf(taskId);
      if (taskIndex >= 0) {                       // mapping
        status.mapProgress = 1.0f;
      } else {
        status.reduceProgress = 1.0f;
      }
    }

    public synchronized void fsError(String message) throws IOException {
      LOG.severe("FSError: "+ message);
    }

  }

  public LocalJobRunner(NutchConf nutchConf) throws IOException {
    this.fs = NutchFileSystem.get(nutchConf);
    this.nutchConf = nutchConf;
  }

  // JobSubmissionProtocol methods

  public JobStatus submitJob(String jobFile) throws IOException {
    return new Job(jobFile, this.nutchConf).status;
  }

  public void killJob(String id) {
    ((Thread)jobs.get(id)).stop();
  }

  public JobProfile getJobProfile(String id) {
    Job job = (Job)jobs.get(id);
    return new JobProfile(id, job.file, "http://localhost:8080/");
  }

  public Vector[] getMapTaskReport(String id) {
    return new Vector[0];
  }
  public Vector[] getReduceTaskReport(String id) {
    return new Vector[0];
  }

  public JobStatus getJobStatus(String id) {
    Job job = (Job)jobs.get(id);
    return job.status;
  }

  public String getFilesystemName() throws IOException {
    return fs.getName();
  }
}
