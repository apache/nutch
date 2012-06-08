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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nutch.api.JobStatus.State;

public interface JobManager {
  
  public static enum JobType {INJECT, GENERATE, FETCH, PARSE, UPDATEDB, INDEX, CRAWL, READDB, CLASS};

  public List<JobStatus> list(String crawlId, State state) throws Exception;
  
  public JobStatus get(String crawlId, String id) throws Exception;
  
  public String create(String crawlId, JobType type, String confId,
      Map<String,Object> args) throws Exception;
  
  public boolean abort(String crawlId, String id) throws Exception;
  
  public boolean stop(String crawlId, String id) throws Exception;
}
