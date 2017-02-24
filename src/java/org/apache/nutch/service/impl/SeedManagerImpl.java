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

import java.util.HashMap;
import java.util.Map;

import org.apache.nutch.service.SeedManager;
import org.apache.nutch.service.model.request.SeedList;

public class SeedManagerImpl implements SeedManager {

  private static Map<String, SeedList> seeds;

  public SeedManagerImpl() {
    seeds = new HashMap<>();
  }

  public SeedList getSeedList(String seedName) {
    if(seeds.containsKey(seedName)) {
      return seeds.get(seedName);
    }
    else
      return null;
  }

  public void setSeedList(String seedName, SeedList seedList) {
    seeds.put(seedName, seedList);
  }

  public Map<String, SeedList> getSeeds(){
    return seeds;
  }
  
  public boolean deleteSeedList(String seedName) {
    if(seeds.containsKey(seedName)) {
      seeds.remove(seedName);
      return true;
    }
    else
      return false;
  }
}
