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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.Indexer;
import org.apache.nutch.indexer.NutchIndexWriterFactory;
import org.apache.nutch.indexer.solr.SolrConstants;
import org.apache.nutch.util.NutchConfiguration;

public class SolrIndexer extends Configured implements Tool {

  public static Log LOG = LogFactory.getLog(SolrIndexer.class);

  public SolrIndexer() {
    super(null);
  }

  public SolrIndexer(Configuration conf) {
    super(conf);
  }

  private void indexSolr(String solrUrl, String table, boolean reindex)
  throws Exception {
    LOG.info("SolrIndexer: starting");

    NutchIndexWriterFactory.addClassToConf(getConf(), SolrWriter.class);
    getConf().set(SolrConstants.SERVER_URL, solrUrl);
    final Job job = Indexer.createIndexJob(getConf(), "solr-index " + table,
        table, reindex);;

    final Path tmp = new Path("tmp_" + System.currentTimeMillis() + "-" +
                         new Random().nextInt());

    FileOutputFormat.setOutputPath(job, tmp);
    try {
        job.waitForCompletion(true);
    } finally {
      FileSystem.get(getConf()).delete(tmp, true);
    }
    LOG.info("SolrIndexer: done");
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: SolrIndexer <solr url> <webtable> [-reindex]");
      return -1;
    }

    boolean reindex = false;
    if (args.length > 2 && "-reindex".equals(args[2])) {
      reindex = true;
    }

    try {
      indexSolr(args[0], args[1], reindex);
      return 0;
    } catch (final Exception e) {
      LOG.fatal("SolrIndexer: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(), new SolrIndexer(), args);
    System.exit(res);
  }
}
