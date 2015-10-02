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

package org.apache.nutch.publisher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.plugin.PluginRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NutchPublishers extends Configured implements NutchPublisher{

  private static final Logger LOG = LoggerFactory.getLogger(NutchPublishers.class);
  private NutchPublisher[] publishers;

  public NutchPublishers(Configuration conf) {
    // TODO Auto-generated constructor stub
    this.publishers = (NutchPublisher[])PluginRepository.get(conf).
        getOrderedPlugins(NutchPublisher.class, 
            NutchPublisher.X_POINT_ID, "publisher.order");
  }

  @Override
  public boolean setConfig(Configuration conf) {
    // TODO Auto-generated method stub
    boolean success = false;
    try {
      for(int i=0; i<this.publishers.length; i++) {
        success |= this.publishers[i].setConfig(conf);
        if(success)
          LOG.info("Successfully loaded " + 
              this.publishers[i].getClass().getName() +" publisher");
      }
    }catch(Exception e) {
      LOG.warn("Error while loading publishers : " + e.getMessage());
    }
    if(!success) {
      LOG.warn("Could not load any publishers out of " + 
          this.publishers.length +" publishers");
    }
    return success;
  }

  @Override
  public void publish(Object event, Configuration conf) {
    // TODO Auto-generated method stub

    for(int i=0; i<this.publishers.length; i++) {
      try{
        this.publishers[i].publish(event, conf);
      }catch(Exception e){
        LOG.warn("Could not post event to " + 
            this.publishers[i].getClass().getName());
      }
    }
  }

  @Override
  public Configuration getConf() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setConf(Configuration arg0) {
    // TODO Auto-generated method stub
  }
}
