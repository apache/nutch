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

package org.apache.nutch.mapReduce;

import org.apache.nutch.io.*;

import java.io.*;
import java.net.*;
import java.util.*;

/** Base class for tasks. */
public abstract class Task implements Writable {

  ////////////////////////////////////////////
  // Fields
  ////////////////////////////////////////////

  private String jobFile;                         // job configuration file
  private String taskId;                          // unique, includes job id

  ////////////////////////////////////////////
  // Constructors
  ////////////////////////////////////////////

  public Task() {}

  public Task(String jobFile, String taskId) {
    this.jobFile = jobFile;
    this.taskId = taskId;
  }

  ////////////////////////////////////////////
  // Accessors
  ////////////////////////////////////////////
  public void setJobFile(String jobFile) { this.jobFile = jobFile; }
  public String getJobFile() { return jobFile; }
  public String getTaskId() { return taskId; }

  ////////////////////////////////////////////
  // Writable methods
  ////////////////////////////////////////////

  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, jobFile);
    UTF8.writeString(out, taskId);
  }
  public void readFields(DataInput in) throws IOException {
    jobFile = UTF8.readString(in);
    taskId = UTF8.readString(in);
  }

  public String toString() { return taskId; }

  /** Run this task as a part of the named job.  This method is executed in the
   * child process and is what invokes user-supplied map, reduce, etc. methods.
   * @param umbilical for progress reports
   */
  public abstract void run(JobConf job, TaskUmbilicalProtocol umbilical)
    throws IOException;


  /** Return an approprate thread runner for this task. */
  public abstract TaskRunner createRunner(TaskTracker tracker);

}
