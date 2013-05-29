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

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.IndexerJob;
import org.apache.nutch.indexer.NutchIndexWriterFactory;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.ToolUtil;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrIndexerJob extends IndexerJob {

  public static Logger LOG = LoggerFactory.getLogger(SolrIndexerJob.class);

  @Override
  public Map<String,Object> run(Map<String,Object> args) throws Exception {
    String solrUrl = (String)args.get(Nutch.ARG_SOLR);
    String batchId = (String)args.get(Nutch.ARG_BATCH);
    NutchIndexWriterFactory.addClassToConf(getConf(), SolrWriter.class);
    getConf().set(SolrConstants.SERVER_URL, solrUrl);

    currentJob = createIndexJob(getConf(), "solr-index", batchId);

    currentJob.waitForCompletion(true);
    ToolUtil.recordJobStatus(null, currentJob, results);
    return results;
  }

  public void indexSolr(String solrUrl, String batchId) throws Exception {
    LOG.info("SolrIndexerJob: starting");

    run(ToolUtil.toArgMap(
        Nutch.ARG_SOLR, solrUrl,
        Nutch.ARG_BATCH, batchId));
    // do the commits once and for all the reducers in one go
    getConf().set(SolrConstants.SERVER_URL,solrUrl);
    SolrServer solr = SolrUtils.getCommonsHttpSolrServer(getConf());
    if (getConf().getBoolean(SolrConstants.COMMIT_INDEX, true)) {
      solr.commit();
    }
    LOG.info("SolrIndexerJob: done.");
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
