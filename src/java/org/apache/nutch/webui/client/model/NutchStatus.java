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
package org.apache.nutch.webui.client.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Date;
import java.util.Set;

public class NutchStatus implements Serializable {

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

  public Collection<JobInfo> getRunningJobs() {
    return runningJobs;
  }

  public void setRunningJobs(Collection<JobInfo> runningJobs) {
    this.runningJobs = runningJobs;
  }
}
