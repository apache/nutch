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
package org.apache.nutch.webui.client.impl;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import java.util.Map;

import org.apache.nutch.webui.client.NutchClient;
import org.apache.nutch.webui.client.model.ConnectionStatus;
import org.apache.nutch.webui.client.model.JobConfig;
import org.apache.nutch.webui.client.model.JobInfo;
import org.apache.nutch.webui.client.model.NutchStatus;
import org.apache.nutch.webui.model.NutchInstance;
import org.apache.nutch.webui.model.SeedList;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class NutchClientImpl implements NutchClient {
  private Client client;
  private WebResource nutchResource;
  private NutchInstance instance;

  public NutchClientImpl(NutchInstance instance) {
    this.instance = instance;
    createClient();
  }

  public void createClient() {
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures()
        .put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
    this.client = Client.create(clientConfig);
    this.nutchResource = client.resource(instance.getUrl());
  }

  @Override
  public NutchStatus getNutchStatus() {
    return nutchResource.path("/admin").type(APPLICATION_JSON)
        .get(NutchStatus.class);
  }

  @Override
  public ConnectionStatus getConnectionStatus() {

    getNutchStatus();
    return ConnectionStatus.CONNECTED;
    // TODO implement disconnected status
  }

  @Override
  public String executeJob(JobConfig jobConfig) {
    return nutchResource.path("/job/create").type(APPLICATION_JSON)
        .post(String.class, jobConfig);
  }

  @Override
  public JobInfo getJobInfo(String jobId) {
    return nutchResource.path("/job/" + jobId).type(APPLICATION_JSON)
        .get(JobInfo.class);
  }

  @Override
  public NutchInstance getNutchInstance() {
    return instance;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, String> getNutchConfig(String config) {
    return nutchResource.path("/config/" + config).type(APPLICATION_JSON)
        .get(Map.class);
  }

  @Override
  public String createSeed(SeedList seedList) {
    return nutchResource.path("/seed/create").type(APPLICATION_JSON)
        .post(String.class, seedList);
  }
}
