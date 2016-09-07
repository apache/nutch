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
package org.apache.nutch.fetcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.publisher.NutchPublishers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles the publishing of the events to the queue implementation. 
 *
 */
public class FetcherThreadPublisher {

  private static NutchPublishers publisher;
  private static final Logger LOG = LoggerFactory.getLogger(FetcherThreadPublisher.class);

  /**
   * Configure all registered publishers
   * @param conf {@link org.apache.hadoop.conf.Configuration Configuration} to be used
   */
  public FetcherThreadPublisher(Configuration conf) {
    LOG.info("Setting up publishers");
    publisher = new NutchPublishers(conf);
    if(!publisher.setConfig(conf))
      publisher = null;
  }

  /**
   * Publish event to all registered publishers
   * @param event	{@link org.apache.nutch.fetcher.FetcherThreadEvent Event} to be published
   * @param conf	{@link org.apache.hadoop.conf.Configuration Configuration} to be used
   */
  public void publish(FetcherThreadEvent event, Configuration conf) {
    if(publisher!=null) {
      publisher.publish(event, conf);
    }
    else {
      LOG.warn("Could not instantiate publisher implementation, continuing without publishing");
    }
  }

}
