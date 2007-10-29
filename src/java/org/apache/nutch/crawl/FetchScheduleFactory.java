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

package org.apache.nutch.crawl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.ObjectCache;

/** Creates and caches a {@link FetchSchedule} implementation. */
public class FetchScheduleFactory {

  public static final Log LOG = LogFactory.getLog(FetchScheduleFactory.class);

  private FetchScheduleFactory() {}                   // no public ctor

  /** Return the FetchSchedule implementation. */
  public static FetchSchedule getFetchSchedule(Configuration conf) {
    String clazz = conf.get("db.fetch.schedule.class", DefaultFetchSchedule.class.getName());
    ObjectCache objectCache = ObjectCache.get(conf);
    FetchSchedule impl = (FetchSchedule)objectCache.getObject(clazz);
    if (impl == null) {
      try {
        LOG.info("Using FetchSchedule impl: " + clazz);
        Class implClass = Class.forName(clazz);
        impl = (FetchSchedule)implClass.newInstance();
        impl.setConf(conf);
        objectCache.setObject(clazz, impl);
      } catch (Exception e) {
        throw new RuntimeException("Couldn't create " + clazz, e);
      }
    }
    return impl;
  }
}
