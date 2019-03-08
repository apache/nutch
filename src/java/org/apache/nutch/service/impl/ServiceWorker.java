/*
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
package org.apache.nutch.service.impl;

import java.lang.invoke.MethodHandles;

import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.service.model.request.ServiceConfig;
import org.apache.nutch.util.NutchTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceWorker implements Runnable {

  private ServiceConfig serviceConfig;
  private NutchTool tool;
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public ServiceWorker(ServiceConfig serviceConfig, NutchTool tool) {
    this.serviceConfig = serviceConfig;
    this.tool = tool;
  }

  @Override
  public void run() {
    try {
      tool.run(serviceConfig.getArgs(), serviceConfig.getCrawlId());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      LOG.error("Error running service worker : {}",
          StringUtils.stringifyException(e));
    }
  }

}
