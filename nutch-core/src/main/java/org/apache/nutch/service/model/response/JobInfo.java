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
package org.apache.nutch.service.model.response;

import java.util.Map;

import org.apache.nutch.service.JobManager.JobType;
import org.apache.nutch.service.model.request.JobConfig;

/**
 * This is the response object containing Job information
 * 
 *
 */
public class JobInfo {

  public static enum State {
    IDLE, RUNNING, FINISHED, FAILED, KILLED, STOPPING, KILLING, ANY
  };

  private String id;
  private JobType type;
  private String confId;
  private Map<String, Object> args;
  private Map<String, Object> result;
  private State state;
  private String msg;
  private String crawlId;

  public JobInfo(String generateId, JobConfig jobConfig, State state,
      String msg) {
    this.id = generateId;
    this.type = jobConfig.getType();
    this.confId = jobConfig.getConfId();
    this.crawlId = jobConfig.getCrawlId();
    this.args = jobConfig.getArgs();
    this.msg = msg;
    this.state = state;
  }
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public JobType getType() {
    return type;
  }
  public void setType(JobType type) {
    this.type = type;
  }
  public String getConfId() {
    return confId;
  }
  public void setConfId(String confId) {
    this.confId = confId;
  }
  public Map<String, Object> getArgs() {
    return args;
  }
  public void setArgs(Map<String, Object> args) {
    this.args = args;
  }
  public Map<String, Object> getResult() {
    return result;
  }
  public void setResult(Map<String, Object> result) {
    this.result = result;
  }	
  public State getState() {
    return state;
  }
  public void setState(State state) {
    this.state = state;
  }
  public String getMsg() {
    return msg;
  }
  public void setMsg(String msg) {
    this.msg = msg;
  }
  public String getCrawlId() {
    return crawlId;
  }
  public void setCrawlId(String crawlId) {
    this.crawlId = crawlId;
  }
}
