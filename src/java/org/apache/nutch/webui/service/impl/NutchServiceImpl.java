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
package org.apache.nutch.webui.service.impl;

import java.net.ConnectException;
import java.util.Collections;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.nutch.webui.client.NutchClientFactory;
import org.apache.nutch.webui.client.model.ConnectionStatus;
import org.apache.nutch.webui.client.model.NutchStatus;
import org.apache.nutch.webui.model.NutchInstance;
import org.apache.nutch.webui.service.NutchInstanceService;
import org.apache.nutch.webui.service.NutchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.sun.jersey.api.client.ClientHandlerException;

@Service
public class NutchServiceImpl implements NutchService {
  private static final Logger logger = LoggerFactory.getLogger(NutchServiceImpl.class);

  @Resource
  private NutchClientFactory nutchClientFactory;

  @Resource
  private NutchInstanceService instanceService;

  @Override
  public ConnectionStatus getConnectionStatus(Long instanceId) {
    NutchInstance instance = instanceService.getInstance(instanceId);
    try {
      NutchStatus nutchStatus = nutchClientFactory.getClient(instance)
          .getNutchStatus();
      if (nutchStatus.getStartDate() != null) {
        return ConnectionStatus.CONNECTED;
      }
    } catch (Exception e) {
      if (e.getCause() instanceof ConnectException) {
        return ConnectionStatus.DISCONNECTED;
      }

      logger.error("Cannot connect to nutch server!", e);
    }
    return null;
  }

  @Override
  public Map<String, String> getNutchConfig(Long instanceId) {
    NutchInstance instance = instanceService.getInstance(instanceId);
    try {
      return nutchClientFactory.getClient(instance).getNutchConfig("default");
    } catch (ClientHandlerException exception) {
      return Collections.emptyMap();
    }
  }

  @Override
  public NutchStatus getNutchStatus(Long instanceId) {
    NutchInstance instance = instanceService.getInstance(instanceId);
    return nutchClientFactory.getClient(instance).getNutchStatus();
  }
}
