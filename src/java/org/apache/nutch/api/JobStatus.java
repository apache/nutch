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
package org.apache.nutch.api;

import java.util.Map;

import org.apache.nutch.api.JobManager.JobType;
import org.apache.nutch.util.NutchTool;

public class JobStatus {
  public static enum State {IDLE, RUNNING, FINISHED, FAILED, KILLED,
    STOPPING, KILLING, ANY};
  public String id;
  public JobType type;
  public String confId;
  public Map<String,Object> args;
  public Map<String,Object> result;
  public NutchTool tool;
  public State state;
  public String msg;
  
  public JobStatus(String id, JobType type, String confId, Map<String,Object> args,
      State state, String msg) {
    this.id = id;
    this.type = type;
    this.confId = confId;
    this.args = args;
    this.state = state;
    this.msg = msg;
  }

}
