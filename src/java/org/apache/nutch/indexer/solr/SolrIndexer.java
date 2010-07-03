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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.IndexerMapReduce;
import org.apache.nutch.indexer.NutchIndexWriterFactory;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SolrIndexer extends Configured implements Tool {

  public static Log LOG = LogFactory.getLog(SolrIndexer.class);

  public SolrIndexer() {
    super(null);
  }

  public SolrIndexer(Configuration conf) {
    super(conf);
  }

  public void indexSolr(String solrUrl, Path crawlDb, Path linkDb,
      List<Path> segments) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("SolrIndexer: starting at " + sdf.format(start));

    final JobConf job = new NutchJob(getConf());
    job.setJobName("index-solr " + solrUrl);

    IndexerMapReduce.initMRJob(crawlDb, linkDb, segments, job);

    job.set(SolrConstants.SERVER_URL, solrUrl);

    NutchIndexWriterFactory.addClassToConf(job, SolrWriter.class);

    job.setReduceSpeculativeExecution(false);

    final Path tmp = new Path("tmp_" + System.currentTimeMillis() + "-" +
                         new Random().nextInt());

    FileOutputFormat.setOutputPath(job, tmp);
    try {
      JobClient.runJob(job);
      // do the commits once and for all the reducers in one go
      SolrServer solr =  new CommonsHttpSolrServer(solrUrl);
      solr.commit();
      long end = System.currentTimeMillis();
      LOG.info("SolrIndexer: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
    }
    catch (Exception e){
      LOG.error(e);
    } finally {
      FileSystem.get(job).delete(tmp, true);
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("Usage: SolrIndexer <solr url> <crawldb> <linkdb> <segment> ...");
      return -1;
    }

    final Path crawlDb = new Path(args[1]);
    final Path linkDb = new Path(args[2]);

    final List<Path> segments = new ArrayList<Path>();
    for (int i = 3; i < args.length; i++) {
      segments.add(new Path(args[i]));
    }

    try {
      indexSolr(args[0], crawlDb, linkDb, segments);
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
