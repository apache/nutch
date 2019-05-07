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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/** A {@link Job} for Nutch jobs. */
public class NutchJob extends Job {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  @SuppressWarnings("deprecation")
  public NutchJob(Configuration conf, String jobName) throws IOException {
    super(conf, jobName);
  }

  public static Job getInstance(Configuration conf) throws IOException {
    return Job.getInstance(conf);
  } 

  /*
   * Clean up the file system in case of a job failure.
   */
  public static void cleanupAfterFailure(Path tempDir, Path lock, FileSystem fs)
         throws IOException {
    try {
      if (fs.exists(tempDir)) {
        fs.delete(tempDir, true);
      }
      LockUtil.removeLockFile(fs, lock);
    } catch (IOException e) {
      LOG.error("NutchJob cleanup failed: {}", e.getMessage());
      throw e;
    }
  }

}
