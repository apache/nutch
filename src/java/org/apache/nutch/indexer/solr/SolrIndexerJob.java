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

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.IndexerJob;
import org.apache.nutch.indexer.NutchIndexWriterFactory;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;

public class SolrIndexerJob extends IndexerJob {

  public static Log LOG = LogFactory.getLog(SolrIndexerJob.class);

  private void indexSolr(String solrUrl, String crawlId) throws Exception {
    LOG.info("SolrIndexerJob: starting");

    NutchIndexWriterFactory.addClassToConf(getConf(), SolrWriter.class);
    getConf().set(SolrConstants.SERVER_URL, solrUrl);

    Job job = createIndexJob(getConf(), "solr-index", crawlId);
    Path tmp = new Path("tmp_" + System.currentTimeMillis() + "-"
                + new Random().nextInt());

    FileOutputFormat.setOutputPath(job, tmp);
    boolean success = false;
    try {
      success = job.waitForCompletion(true);
      // do the commits once and for all the reducers in one go
      SolrServer solr = new CommonsHttpSolrServer(solrUrl);
      solr.commit();
    } finally {
      FileSystem.get(getConf()).delete(tmp, true);
    }
    LOG.info("SolrIndexerJob: " + (success ? "done" : "failed"));
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: SolrIndexerJob <solr url> (<crawl id> | -all | -reindex)");
      return -1;
    }

    try {
      indexSolr(args[0], args[1]);
      return 0;
    } catch (final Exception e) {
      LOG.fatal("SolrIndexerJob: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new SolrIndexerJob(), args);
    System.exit(res);
  }
}
