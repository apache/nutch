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
package org.apache.nutch.api.impl;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.api.ConfManager;
import org.apache.nutch.api.model.request.NutchConfig;
import org.apache.nutch.api.resources.ConfigResource;
import org.apache.nutch.util.NutchConfiguration;

import com.google.common.collect.Maps;

/**
 * Configuration manager which holds a map of {@link Configuration} type configurations and ids.
 */
public class RAMConfManager implements ConfManager {
  private Map<String, Configuration> configurations = Maps.newConcurrentMap();

  private AtomicInteger newConfigId = new AtomicInteger();

  /**
   * Public constructor which creates a default configuration with id of {@link ConfigResource#DEFAULT}.
   */
  public RAMConfManager() {
    configurations.put(ConfigResource.DEFAULT, NutchConfiguration.create());
  }

  /**
   * Public constructor which accepts a configuration id and {@link Configuration} type configuration.
   *
   * @param confId configuration id
   * @param configuration configuration
   */
  public RAMConfManager(String confId, Configuration configuration) {
    configurations.put(confId, configuration);
  }

  /**
   * Lists configuration keys.
   *
   * @return Set of configuration keys
   */
  public Set<String> list() {
    return configurations.keySet();
  }

  /**
   * Returns configuration map for give configuration id.
   *
   * @param confId Configuration id.
   * @return Configuration for given configuration id.
   * {@link ConfigResource#DEFAULT} is used if given configuration id is null.
   */
  public Configuration get(String confId) {
    if (confId == null) {
      return configurations.get(ConfigResource.DEFAULT);
    }
    return configurations.get(confId);
  }

  /**
   * Returns configuration map for give configuration id.
   * An empty map is returned if a configuration could not be retrieved for given configuration id.
   *
   * @param confId Configuration id
   * @return map of configurations
   */
  public Map<String, String> getAsMap(String confId) {
    Configuration configuration = configurations.get(confId);
    if (configuration == null) {
      return Collections.emptyMap();
    }

    Iterator<Entry<String, String>> iterator = configuration.iterator();
    Map<String, String> configMap = Maps.newTreeMap();
    while (iterator.hasNext()) {
      Entry<String, String> entry = iterator.next();
      configMap.put(entry.getKey(), entry.getValue());
    }
    return configMap;
  }

  /**
   * Sets a property for the configuration which has given configuration id.
   *
   * @param confId Configuration id
   * @param propName property name to set
   * @param propValue property value to set
   */
  public void setProperty(String confId, String propName, String propValue) {
    if (!configurations.containsKey(confId)) {
      throw new IllegalArgumentException("Unknown configId '" + confId + "'");
    }
    Configuration conf = configurations.get(confId);
    conf.set(propName, propValue);
  }

  /**
   * Deletes configuration for given configuration id.
   *
   * @param confId Configuration id
   */
  public void delete(String confId) {
    configurations.remove(confId);
  }

  /**
   * Creates hadoop configuration for given Nutch configuration.
   * Checks whether it can create a Nutch configuration or not before it creates.
   * Throws {@link IllegalArgumentException} if can not pass {{@link #canCreate(NutchConfig)}}.
   *
   * @param nutchConfig Nutch configuration
   * @return created configuration id
   */
  @Override
  public String create(NutchConfig nutchConfig) {
    if (StringUtils.isBlank(nutchConfig.getConfigId())) {
      nutchConfig.setConfigId(String.valueOf(newConfigId.incrementAndGet()));
    }

    if (!canCreate(nutchConfig)) {
      throw new IllegalArgumentException("Config already exists.");
    }

    createHadoopConfig(nutchConfig);
    return nutchConfig.getConfigId();
  }

  /**
   * Checks can create a Nutch configuration or not.
   *
   * @param nutchConfig Nutch configuration
   * @return True if forcing is enabled at Nutch configuration.
   * Otherwise makes a check based on whether there is an existing configuration at configuration set
   * with same configuration id of given Nutch configuration.
   */
  private boolean canCreate(NutchConfig nutchConfig) {
    if (nutchConfig.isForce()) {
      return true;
    }
    if (!configurations.containsKey(nutchConfig.getConfigId())) {
      return true;
    }
    return false;
  }

  /**
   * Creates a Hadoop configuration from given Nutch configuration.
   *
   * @param nutchConfig Nutch configuration.
   */
  private void createHadoopConfig(NutchConfig nutchConfig) {
    Configuration conf = NutchConfiguration.create();
    configurations.put(nutchConfig.getConfigId(), conf);

    if (MapUtils.isEmpty(nutchConfig.getParams())) {
      return;
    }
    for (Entry<String, String> e : nutchConfig.getParams().entrySet()) {
      conf.set(e.getKey(), e.getValue());
    }
  }

}
