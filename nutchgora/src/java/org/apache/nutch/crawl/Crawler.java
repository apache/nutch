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
package org.apache.nutch.crawl;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.indexer.solr.SolrIndexerJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Crawler extends NutchTool implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(Crawler.class);
  
  private boolean cleanSeedDir = false;
  private String tmpSeedDir = null;
  private HashMap<String,Object> results = new HashMap<String,Object>();
  private Map<String,Object> status =
    Collections.synchronizedMap(new HashMap<String,Object>());
  private NutchTool currentTool = null;
  private boolean shouldStop = false;
  
  @Override
  public Map<String,Object> getStatus() {
    return status;
  }
  
  private Map<String,Object> runTool(Class<? extends NutchTool> toolClass,
      Map<String,Object> args) throws Exception {
    currentTool = (NutchTool)ReflectionUtils.newInstance(toolClass, getConf());
    return currentTool.run(args);
  }
  
  @Override
  public boolean stopJob() throws Exception {
    shouldStop = true;
    if (currentTool != null) {
      return currentTool.stopJob();
    }
    return false;
  }

  @Override
  public boolean killJob() throws Exception {
    shouldStop = true;
    if (currentTool != null) {
      return currentTool.killJob();
    }
    return false;
  }

  @Override
  public Map<String,Object> run(Map<String, Object> args) throws Exception {
    results.clear();
    status.clear();
    String crawlId = (String)args.get(Nutch.ARG_CRAWL);
    if (crawlId != null) {
      getConf().set(Nutch.CRAWL_ID_KEY, crawlId);
    }
    String seedDir = null;
    String seedList = (String)args.get(Nutch.ARG_SEEDLIST);    
    if (seedList != null) { // takes precedence
      String[] seeds = seedList.split("\\s+");
      // create tmp. dir
      String tmpSeedDir = getConf().get("hadoop.tmp.dir") + "/seed-" +
        System.currentTimeMillis();
      FileSystem fs = FileSystem.get(getConf());
      Path p = new Path(tmpSeedDir);
      fs.mkdirs(p);
      Path seedOut = new Path(p, "urls");
      OutputStream os = fs.create(seedOut);
      for (String s : seeds) {
        os.write(s.getBytes());
        os.write('\n');
      }
      os.flush();
      os.close();
      cleanSeedDir = true;
      seedDir = tmpSeedDir;
    } else {
      seedDir = (String)args.get(Nutch.ARG_SEEDDIR);
    }
    Integer depth = (Integer)args.get(Nutch.ARG_DEPTH);
    if (depth == null) depth = 1;
    boolean parse = getConf().getBoolean(FetcherJob.PARSE_KEY, false);
    String solrUrl = (String)args.get(Nutch.ARG_SOLR);
    int onePhase = 3;
    if (!parse) onePhase++;
    float totalPhases = depth * onePhase;
    if (seedDir != null) totalPhases++;
    float phase = 0;
    Map<String,Object> jobRes = null;
    LinkedHashMap<String,Object> subTools = new LinkedHashMap<String,Object>();
    status.put(Nutch.STAT_JOBS, subTools);
    results.put(Nutch.STAT_JOBS, subTools);
    // inject phase
    if (seedDir != null) {
      status.put(Nutch.STAT_PHASE, "inject");
      jobRes = runTool(InjectorJob.class, args);
      if (jobRes != null) {
        subTools.put("inject", jobRes);
      }
      status.put(Nutch.STAT_PROGRESS, ++phase / totalPhases);
      if (cleanSeedDir && tmpSeedDir != null) {
        LOG.info(" - cleaning tmp seed list in " + tmpSeedDir);
        FileSystem.get(getConf()).delete(new Path(tmpSeedDir), true);
      }
    }
    if (shouldStop) {
      return results;
    }
    // run "depth" cycles
    for (int i = 0; i < depth; i++) {
      status.put(Nutch.STAT_PHASE, "generate " + i);
      jobRes = runTool(GeneratorJob.class, args);
      if (jobRes != null) {
        subTools.put("generate " + i, jobRes);
      }
      status.put(Nutch.STAT_PROGRESS, ++phase / totalPhases);
      if (shouldStop) {
        return results;
      }
      status.put(Nutch.STAT_PHASE, "fetch " + i);
      jobRes = runTool(FetcherJob.class, args);
      if (jobRes != null) {
        subTools.put("fetch " + i, jobRes);
      }
      status.put(Nutch.STAT_PROGRESS, ++phase / totalPhases);
      if (shouldStop) {
        return results;
      }
      if (!parse) {
        status.put(Nutch.STAT_PHASE, "parse " + i);
        jobRes = runTool(ParserJob.class, args);
        if (jobRes != null) {
          subTools.put("parse " + i, jobRes);
        }
        status.put(Nutch.STAT_PROGRESS, ++phase / totalPhases);
        if (shouldStop) {
          return results;
        }
      }
      status.put(Nutch.STAT_PHASE, "updatedb " + i);
      jobRes = runTool(DbUpdaterJob.class, args);
      if (jobRes != null) {
        subTools.put("updatedb " + i, jobRes);
      }
      status.put(Nutch.STAT_PROGRESS, ++phase / totalPhases);
      if (shouldStop) {
        return results;
      }
    }
    if (solrUrl != null) {
      status.put(Nutch.STAT_PHASE, "index");
      jobRes = runTool(SolrIndexerJob.class, args);
      if (jobRes != null) {
        subTools.put("index", jobRes);
      }
    }
    return results;
  }

  @Override
  public float getProgress() {
    Float p = (Float)status.get(Nutch.STAT_PROGRESS);
    if (p == null) return 0;
    return p;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      System.out.println("Usage: Crawler (<seedDir> | -continue) [-solr <solrURL>] [-threads n] [-depth i] [-topN N] [-numTasks N]");
      return -1;
    }
    // parse most common arguments here
    String seedDir = null;
    int threads = getConf().getInt("fetcher.threads.fetch", 10);    
    int depth = 5;
    long topN = Long.MAX_VALUE;
    String solrUrl = null;
    Integer numTasks = null;
    
    for (int i = 0; i < args.length; i++) {
      if ("-threads".equals(args[i])) {
        threads = Integer.parseInt(args[i+1]);
        i++;
      } else if ("-depth".equals(args[i])) {
        depth = Integer.parseInt(args[i+1]);
        i++;
      } else if ("-topN".equals(args[i])) {
          topN = Integer.parseInt(args[i+1]);
          i++; 
      } else if ("-solr".equals(args[i])) {
        solrUrl = StringUtils.lowerCase(args[i + 1]);
        i++;
      } else if ("-numTasks".equals(args[i])) {
        numTasks = Integer.parseInt(args[i+1]);
        i++;
      } else if ("-continue".equals(args[i])) {
        // skip
      } else if (args[i] != null) {
        seedDir = args[i];
      }
    }
    Map<String,Object> argMap = ToolUtil.toArgMap(
        Nutch.ARG_THREADS, threads,
        Nutch.ARG_DEPTH, depth,
        Nutch.ARG_TOPN, topN,
        Nutch.ARG_SOLR, solrUrl,
        Nutch.ARG_SEEDDIR, seedDir,
        Nutch.ARG_NUMTASKS, numTasks);
    run(argMap);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    Crawler c = new Crawler();
    Configuration conf = NutchConfiguration.create();
    int res = ToolRunner.run(conf, c, args);
    System.exit(res);
  }
}
