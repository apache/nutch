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

public class NutchStatus {
  private Date startDate;
  private Set<String> configuration;
  private Collection<JobInfo> jobs;
  private Collection<JobInfo> runningJobs;

  public Date getStartDate() {
    return startDate;
  }

  public void setStartDate(Date startDate) {
    this.startDate = startDate;
  }

  public Set<String> getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Set<String> configuration) {
    this.configuration = configuration;
  }

  public Collection<JobInfo> getJobs() {
    return jobs;
  }

  public void setJobs(Collection<JobInfo> jobs) {
    this.jobs = jobs;
  }

  public Collection<JobInfo> getRunningJobs()
  {
    return purgeFinishedFailedJobs(runningJobs);
  }


  public void setRunningJobs(Collection<JobInfo> runningJobs) {
    this.runningJobs = runningJobs;
  }

  private Collection<JobInfo> purgeFinishedFailedJobs(Collection<JobInfo> runningJobColl)
  {
    if (CollectionUtils.isNotEmpty(runningJobColl)) {
      Iterator<JobInfo> runningJobsIterator = runningJobColl.iterator();
      while (runningJobsIterator.hasNext()) {
        JobInfo jobInfo = runningJobsIterator.next();

        if (jobInfo.getState().equals(State.FINISHED)) {
          runningJobsIterator.remove();
        }
        else if (jobInfo.getState().equals(State.FAILED)) {
          runningJobsIterator.remove();
        }

      }
    }
    return runningJobColl;
  }
}
