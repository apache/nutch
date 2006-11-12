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
package org.apache.nutch.clustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.plugin.PluginRuntimeException;
import org.apache.nutch.util.NutchConfiguration;

import junit.framework.TestCase;

public class TestOnlineClustererFactory extends TestCase {

  private Configuration conf;
  
  protected void setUp() throws Exception {
      conf = NutchConfiguration.create();
      conf.set("plugin.includes", ".*");
  }
  
  public void testFacotyr(){
    OnlineClustererFactory factory = new OnlineClustererFactory(conf);
    
    try{
      OnlineClusterer clusterer1=factory.getOnlineClusterer();
      OnlineClusterer clusterer2=factory.getOnlineClusterer();
      assertNotNull(clusterer1);
      assertNotNull(clusterer2);
      
      //Current implementation creates new object instance in every call
      //TODO: check if this is required  
      assertNotSame(clusterer1, clusterer2);
    } catch (PluginRuntimeException pre) {
      fail("Should not throw Exception:" + pre);
    }
  }
}
