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
package org.apache.nutch.webui.client;

import java.util.Map;

import org.apache.nutch.webui.client.model.ConnectionStatus;
import org.apache.nutch.webui.client.model.JobConfig;
import org.apache.nutch.webui.client.model.JobInfo;
import org.apache.nutch.webui.client.model.NutchStatus;
import org.apache.nutch.webui.model.NutchInstance;
import org.apache.nutch.webui.model.SeedList;

public interface NutchClient {

  public NutchInstance getNutchInstance();

  public NutchStatus getNutchStatus();

  public ConnectionStatus getConnectionStatus();

  public String executeJob(JobConfig jobConfig);

  public JobInfo getJobInfo(String jobId);
  
  public Map<String, String> getNutchConfig(String config);

  /**
   * Create seed list and return seed directory location
   * 
   * @param seedList
   * @return
   */
  public String createSeed(SeedList seedList);
}
