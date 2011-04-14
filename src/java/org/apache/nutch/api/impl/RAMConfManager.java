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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.api.ConfManager;
import org.apache.nutch.api.ConfResource;
import org.apache.nutch.util.NutchConfiguration;

public class RAMConfManager implements ConfManager {
  Map<String,Configuration> configs = new HashMap<String,Configuration>();
  
  public RAMConfManager() {
    configs.put(ConfResource.DEFAULT_CONF, NutchConfiguration.create());
  }
  
  public Set<String> list() {
    return configs.keySet();
  }
  
  public Configuration get(String confId) {
    return configs.get(confId);
  }
  
  public Map<String,String> getAsMap(String confId) {
    Configuration cfg = configs.get(confId);
    if (cfg == null) return null;
    Iterator<Entry<String,String>> it = cfg.iterator();
    TreeMap<String,String> res = new TreeMap<String,String>();
    while (it.hasNext()) {
      Entry<String,String> e = it.next();
      res.put(e.getKey(), e.getValue());
    }
    return res;
  }
  
  public void create(String confId, Map<String,String> props, boolean force) throws Exception {
    if (configs.containsKey(confId) && !force) {
      throw new Exception("Config name '" + confId + "' already exists.");
    }
    Configuration conf = NutchConfiguration.create();
    // apply overrides
    if (props != null) {
      for (Entry<String,String> e : props.entrySet()) {
        conf.set(e.getKey(), e.getValue());
      }
    }
    configs.put(confId, conf);
  }
  
  public void setProperty(String confId, String propName, String propValue) throws Exception {
    if (!configs.containsKey(confId)) {
      throw new Exception("Unknown configId '" + confId + "'");
    }
    Configuration conf = configs.get(confId);
    conf.set(propName, propValue);
  }
  
  public void delete(String confId) {
    configs.remove(confId);
  }
}
