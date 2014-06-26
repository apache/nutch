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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.nutch.api.JobManager.JobType;
import org.apache.nutch.crawl.DbUpdaterJob;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.InjectorJob;
import org.apache.nutch.crawl.WebTableReader;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.util.NutchTool;

import com.google.common.collect.Maps;

public class JobFactory {
  private static Map<JobType, Class<? extends NutchTool>> typeToClass;

  static {
    typeToClass = Maps.newHashMap();
    typeToClass.put(JobType.FETCH, FetcherJob.class);
    typeToClass.put(JobType.GENERATE, GeneratorJob.class);
    typeToClass.put(JobType.INDEX, IndexingJob.class);
    typeToClass.put(JobType.INJECT, InjectorJob.class);
    typeToClass.put(JobType.PARSE, ParserJob.class);
    typeToClass.put(JobType.UPDATEDB, DbUpdaterJob.class);
    typeToClass.put(JobType.READDB, WebTableReader.class);
  }

  public NutchTool createToolByType(JobType type, Configuration conf) {
    if (!typeToClass.containsKey(type)) {
      return null;
    }
    Class<? extends NutchTool> clz = typeToClass.get(type);
    return createTool(clz, conf);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public NutchTool createToolByClassName(String className, Configuration conf) {
    try {
      Class clz = Class.forName(className);
      return createTool(clz, conf);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  private NutchTool createTool(Class<? extends NutchTool> clz,
      Configuration conf) {
    return ReflectionUtils.newInstance(clz, conf);
  }

}
