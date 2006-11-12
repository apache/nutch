/*
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

package org.apache.nutch.plugin;

import org.apache.hadoop.conf.Configuration;

/**
 * Simple Test plugin
 * 
 * @author joa23
 */
public class SimpleTestPlugin extends Plugin {

  /**
   * @param pDescriptor 
   * @param conf 
   */
  public SimpleTestPlugin(PluginDescriptor pDescriptor, Configuration conf) {

    super(pDescriptor, conf);
  }

  /*
   * @see org.apache.nutch.plugin.Plugin#startUp()
   */
  public void startUp() throws PluginRuntimeException {
    System.err.println("start up Plugin: " + getDescriptor().getPluginId());

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.nutch.plugin.Plugin#shutDown()
   */
  public void shutDown() throws PluginRuntimeException {
    System.err.println("shutdown Plugin: " + getDescriptor().getPluginId());

  }

}

