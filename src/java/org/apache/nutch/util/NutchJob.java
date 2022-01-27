/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.util;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.nutch.plugin.PluginRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link Job} for Nutch jobs. */
public class NutchJob extends Job {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final String JOB_FAILURE_LOG_FORMAT = "%s job did not succeed, job id: %s, job status: %s, reason: %s";

  @SuppressWarnings("deprecation")
  public NutchJob(Configuration conf, String jobName) throws IOException {
    super(conf, jobName);
    if (conf != null) {
      // initialize plugins early to register URL stream handlers to support
      // custom protocol implementations
      PluginRepository.get(conf);
    }
  }

  public static Job getInstance(Configuration conf) throws IOException {
    return Job.getInstance(conf);
  } 

  /**
   * Clean up the file system in case of a job failure.
   * @param tempDir The temporary directory which needs to be 
   * deleted/cleaned-up.
   * @param fs The {@link org.apache.hadoop.fs.FileSystem} on which 
   * the tempDir resides.
   * @throws IOException Occurs if there is fatal I/O error whilst performing
   * the cleanup.
   */
  public static void cleanupAfterFailure(Path tempDir, FileSystem fs)
      throws IOException {
    cleanupAfterFailure(tempDir, null, fs);
  }

  /**
   * Clean up the file system in case of a job failure.
   * @param tempDir The temporary directory which needs to be 
   * deleted/cleaned-up.
   * @param lock A lockfile if one exists.
   * @see LockUtil#removeLockFile(FileSystem, Path)
   * @param fs The {@link org.apache.hadoop.fs.FileSystem} on which 
   * the tempDir resides.
   * @throws IOException Occurs if there is fatal I/O error whilst performing
   * the cleanup.
   */
  public static void cleanupAfterFailure(Path tempDir, Path lock, FileSystem fs)
         throws IOException {
    try {
      if (fs.exists(tempDir)) {
        fs.delete(tempDir, true);
      }
      if (lock != null) {
        LockUtil.removeLockFile(fs, lock);
      }
    } catch (IOException e) {
      LOG.error("NutchJob cleanup failed: {}", e.getMessage());
      throw e;
    }
  }

  /**
   * Method to return job failure log message. To be used across all Jobs
   * 
   * @param name
   *          Name/Type of the job
   * @param job
   *          Job Object for Job details
   * @return job failure log message
   * @throws IOException
   *           Can occur during fetching job status
   * @throws InterruptedException
   *           Can occur during fetching job status
   */
  public static String getJobFailureLogMessage(String name, Job job)
      throws IOException, InterruptedException {
    if (job != null) {
      return String.format(JOB_FAILURE_LOG_FORMAT, name, job.getJobID(),
          job.getStatus().getState(), job.getStatus().getFailureInfo());
    }
    return "";
  }

}
