/**
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

import org.apache.avro.util.Utf8;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.nutch.metadata.Nutch;

import java.io.IOException;

/** A {@link Job} for Nutch jobs. */
public class NutchJob extends Job {

  /**
   * 
   * @param conf
   * @throws IOException
   * @deprecated use {@link NutchJob#getInstance(Configuration)}
   */
  @Deprecated
  public NutchJob(Configuration conf) throws IOException {
    super(conf);
    super.setJarByClass(this.getClass());
  }

  /**
   * 
   * @param conf
   * @param jobName
   * @throws IOException
   * @deprecated use {@link NutchJob#getInstance(Configuration, String)}
   */
  @Deprecated
  public NutchJob(Configuration conf, String jobName) throws IOException {
    super(conf, jobName);
    // prefix jobName with crawlId if not empty
    String crawlId = conf.get("storage.crawl.id");
    if (!StringUtils.isEmpty(crawlId)) {
      jobName = "[" + crawlId + "]" + jobName;
      setJobName(jobName);
    }
    super.setJarByClass(this.getClass());
  }
  
  /**
   * Creates a new {@link NutchJob} with no particular {@link org.apache.hadoop.mapreduce.Cluster} and a 
   * given {@link org.apache.hadoop.conf.Configuration}.
   * 
   * The <code>NutchJob</code> makes a copy of the <code>Configuration</code> so
   * that any necessary internal modifications do not reflect on the incoming 
   * parameter.
   * 
   * A Cluster will be created from the conf parameter only when it's needed.
   * 
   * This code heavily mimics that of Hadoop's.
   * 
   * @param conf the configuration
   * @return the {@link NutchJob} , with no connection to a cluster yet.
   * @throws IOException
   */
  public static NutchJob getInstance(Configuration conf) throws IOException {
    // create with a null Cluster
    NutchJobConf jobConf = new NutchJobConf(conf);
    return new NutchJob(jobConf);
  }

      
  /**
   * Creates a new {@link NutchJob} with no particular {@link org.apache.hadoop.mapreduce.Cluster} 
   * and a given jobName.
   * A Cluster will be created from the conf parameter only when it's needed.
   *
   * The <code>NutchJob</code> makes a copy of the <code>Configuration</code> so
   * that any necessary internal modifications do not reflect on the incoming 
   * parameter.
   * 
   * @param conf the configuration
   * @param jobName the name given to this NutchJob
   * @return the {@link NutchJob} , with no connection to a cluster yet.
   * @throws IOException
   */
  public static NutchJob getInstance(Configuration conf, String jobName)
           throws IOException {
    // create with a null Cluster
    NutchJob result = getInstance(conf);
    // prefix jobName with crawlId if not empty
    String crawlId = conf.get("storage.crawl.id");
    if (!StringUtils.isEmpty(crawlId)) {
      jobName = "[" + crawlId + "]" + jobName;
      result.setJobName(jobName);
    }
    return result;
  }

  @Override
  public boolean waitForCompletion(boolean verbose) throws IOException,
      InterruptedException, ClassNotFoundException {
    boolean succeeded = super.waitForCompletion(verbose);
    if (!succeeded) {
      // check if we want to fail whenever a job fails. (expert setting)
      if (getConfiguration().getBoolean("fail.on.job.failure", true)) {
        throw new RuntimeException("job failed: " + "name=" + getJobName()
            + ", jobid=" + getJobID());
      }
    }
    return succeeded;
  }

  public static boolean shouldProcess(CharSequence mark, Utf8 batchId) {
    if (mark == null) {
      return false;
    }
    boolean isAll = batchId.equals(Nutch.ALL_CRAWL_ID);
    if (!isAll && !mark.equals(batchId)) {
      return false;
    }
    return true;
  }
}