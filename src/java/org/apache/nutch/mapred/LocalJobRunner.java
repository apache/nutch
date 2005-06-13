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

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;

/** Implements MapReduce locally, in-process, for debugging. */ 
public class LocalJobRunner implements JobSubmissionProtocol {

  private NutchFileSystem fs;
  private HashMap jobs = new HashMap();

  private class Job extends Thread
    implements TaskUmbilicalProtocol {
    private String file;
    private String id;
    private JobConf job;

    private boolean mapping = true;
    private JobStatus status = new JobStatus();

    public Job(String file) throws IOException {
      this.file = file;
      this.id = "job_" + newId();

      File localFile = new File(JobConf.getLocalDir(), id+".xml");
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
        String mapIds[] = new String[splits.length];
        for (int i = 0; i < mapIds.length; i++) {
          mapIds[i] = "map_" + newId();
          MapTask map = new MapTask(file, mapIds[i], splits[i]);
          map.run(job, this);
        }

        // move map output to reduce input
        String reduceId = "_" + newId();
        for (int i = 0; i < mapIds.length; i++) {
          fs.rename(MapOutputFile.getOutputFile(mapIds[i], 0),
                    MapOutputFile.getInputFile(mapIds[i], reduceId));
          MapOutputFile.removeAll(mapIds[i]);
        }

        // run a single reduce task
        ReduceTask reduce = new ReduceTask(file, reduceId, mapIds, 0);
        reduce.run(job, this);
        MapOutputFile.removeAll(reduceId);
        
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

    public void progress(String taskid, FloatWritable progress) {
      if (mapping) {
        status.mapProgress = progress.get();
      } else {
        status.reduceProgress = progress.get();
      }
    }

    public void done(String taskid) throws IOException {}


  }

  public LocalJobRunner() throws IOException {
    this.fs = NutchFileSystem.get();
  }

  // JobSubmissionProtocol methods

  public JobStatus submitJob(String jobFile) throws IOException {
    return new Job(jobFile).status;
  }

  public void killJob(String id) {
    ((Thread)jobs.get(id)).stop();
  }

  public JobProfile getJobProfile(String id) {
    Job job = (Job)jobs.get(id);
    return new JobProfile(id, job.file, "http://localhost:8080/");
  }

  public JobStatus getJobStatus(String id) {
    Job job = (Job)jobs.get(id);
    return job.status;
  }

  public String getFilesystemName() throws IOException {
    return fs.getName();
  }
}
