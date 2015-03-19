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
package org.apache.nutch.service.impl;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.service.ConfManager;
import org.apache.nutch.service.model.request.NutchConfig;
import org.apache.nutch.service.resources.ConfigResource;
import org.apache.nutch.util.NutchConfiguration;

import com.google.common.collect.Maps;

public class ConfManagerImpl implements ConfManager {
	

	private Map<String, Configuration> configurations = Maps.newConcurrentMap();

	private AtomicInteger newConfigId = new AtomicInteger();

	public ConfManagerImpl() {
		configurations.put(ConfigResource.DEFAULT, NutchConfiguration.create());
	}
	
	/**
	 * Returns the configuration associatedConfManagerImpl with the given confId
	 */
	public Configuration get(String confId) {
	    if (confId == null) {
	      return configurations.get(ConfigResource.DEFAULT);
	    }
	    return configurations.get(confId);
	  }

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
	 * Sets the given property in the configuration associated with the confId
	 */
	public void setProperty(String confId, String propName, String propValue) {
	    if (!configurations.containsKey(confId)) {
	      throw new IllegalArgumentException("Unknown configId '" + confId + "'");
	    }
	    Configuration conf = configurations.get(confId);
	    conf.set(propName, propValue);
	}

	public Set<String> list() {
	    return configurations.keySet();
	}

	/**
	 * Created a new configuration based on the values provided.
	 * @param NutchConfig
	 * @return String - confId
	 */
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

	
	public void delete(String confId) {
	    configurations.remove(confId);
	}
	
	private boolean canCreate(NutchConfig nutchConfig) {
	    if (nutchConfig.isForce()) {
	      return true;
	    }
	    if (!configurations.containsKey(nutchConfig.getConfigId())) {
	      return true;
	    }
	    return false;
	}
	
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
