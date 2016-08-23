/*******************************************************************************
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
 ******************************************************************************/
package org.apache.nutch.api.model.response;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.nutch.api.model.response.JobInfo.State;

/**
 * Information object for status of {@link org.apache.nutch.api.NutchServer}.
 * Gives information about when server is started, its configurations, jobs, running jobs
 * and active configuration id.
 *
 * @see org.apache.nutch.api.NutchServer
 */
public class NutchStatus {
  private Date startDate;
  private Set<String> configuration;
  private Collection<JobInfo> jobs;
  private Collection<JobInfo> runningJobs;
  private String activeConfId;

  /**
   * Gets start date of the {@link org.apache.nutch.api.NutchServer}
   *
   * @return start date of the server
   */
  public Date getStartDate() {
    return startDate;
  }

  /**
   * Sets start date of the {@link org.apache.nutch.api.NutchServer}
   *
   * @param startDate start date
   */
  public void setStartDate(Date startDate) {
    this.startDate = startDate;
  }

  /**
   * Gets configuration ids
   *
   * @return configuration ids
   */
  public Set<String> getConfiguration() {
    return configuration;
  }

  /**
   * Sets configuration ids
   *
   * @param configuration configuration ids
   */
  public void setConfiguration(Set<String> configuration) {
    this.configuration = configuration;
  }

  /**
   * Gets jobs
   *
   * @return jobs
   */
  public Collection<JobInfo> getJobs() {
    return jobs;
  }

  /**
   * Sets jobs
   * @param jobs jobs
   */
  public void setJobs(Collection<JobInfo> jobs) {
    this.jobs = jobs;
  }

  /**
   * Gets running jobs
   *
   * @return running jobs
   */
  public Collection<JobInfo> getRunningJobs() {
    return purgeFinishedFailedJobs(runningJobs);
  }

  /**
   * Sets running jobs
   *
   * @param runningJobs running jobs
   */
  public void setRunningJobs(Collection<JobInfo> runningJobs) {
    this.runningJobs = runningJobs;
  }

  /**
   * Gets active configuration id
   *
   * @return active configuration id
   */
  public String getActiveConfId() {
    return activeConfId;
  }

  /**
   * Sets active configuration id
   *
   * @param activeConfId active configuration id
   */
  public void setActiveConfId(String activeConfId) {
    this.activeConfId = activeConfId;
  }

  private Collection<JobInfo> purgeFinishedFailedJobs(
      Collection<JobInfo> runningJobColl) {
    if (CollectionUtils.isNotEmpty(runningJobColl)) {
      Iterator<JobInfo> runningJobsIterator = runningJobColl.iterator();
      while (runningJobsIterator.hasNext()) {
        JobInfo jobInfo = runningJobsIterator.next();

        if (jobInfo.getState().equals(State.FINISHED)) {
          runningJobsIterator.remove();
        } else if (jobInfo.getState().equals(State.FAILED)) {
          runningJobsIterator.remove();
        }

      }
    }
    return runningJobColl;
  }
}
