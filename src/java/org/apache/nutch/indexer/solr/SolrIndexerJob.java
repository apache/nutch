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
package org.apache.nutch.indexer.solr;

import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.IndexerJob;
import org.apache.nutch.indexer.NutchIndexWriterFactory;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchTool;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;

public class SolrIndexerJob extends IndexerJob implements NutchTool {

  public static Logger LOG = LoggerFactory.getLogger(SolrIndexerJob.class);

  public Map<String,Object> prepare() throws Exception {
    return null;
  }
  
  public Map<String,Object> postJob(int jobIndex, Job job) throws Exception {
    return null;
  }
 
  public Map<String,Object> finish() throws Exception {
    return null;
  }
  
  public Job[] createJobs(Object... args) throws Exception {
    String solrUrl = (String)args[0];
    String batchId = (String)args[1];
    NutchIndexWriterFactory.addClassToConf(getConf(), SolrWriter.class);
    getConf().set(SolrConstants.SERVER_URL, solrUrl);

    Job job = createIndexJob(getConf(), "solr-index", batchId);
    Path tmp = new Path("tmp_" + System.currentTimeMillis() + "-"
                + new Random().nextInt());

    FileOutputFormat.setOutputPath(job, tmp);
    return new Job[]{job};
  }

  private void indexSolr(String solrUrl, String crawlId) throws Exception {
    LOG.info("SolrIndexerJob: starting");

    Job[] jobs = createJobs(solrUrl, crawlId);
    boolean success = false;
    try {
      success = jobs[0].waitForCompletion(true);
      // do the commits once and for all the reducers in one go
      SolrServer solr = new CommonsHttpSolrServer(solrUrl);
      solr.commit();
    } finally {
      FileSystem.get(getConf()).delete(FileOutputFormat.getOutputPath(jobs[0]), true);
    }
    LOG.info("SolrIndexerJob: " + (success ? "done" : "failed"));
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: SolrIndexerJob <solr url> (<batchId> | -all | -reindex) [-crawlId <id>]");
      return -1;
    }

    if (args.length == 4 && "-crawlId".equals(args[2])) {
      getConf().set(Nutch.CRAWL_ID_KEY, args[3]);
    }
    try {
      indexSolr(args[0], args[1]);
      return 0;
    } catch (final Exception e) {
      LOG.error("SolrIndexerJob: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new SolrIndexerJob(), args);
    System.exit(res);
  }
}
