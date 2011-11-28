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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.apache.solr.client.solrj.SolrServer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SolrIndexer extends Configured implements Tool {

  public static Logger LOG = LoggerFactory.getLogger(SolrIndexer.class);

  public SolrIndexer() {
    super(null);
  }

  public SolrIndexer(Configuration conf) {
    super(conf);
  }

  public void indexSolr(String solrUrl, Path crawlDb, Path linkDb,
      List<Path> segments) throws IOException {
      indexSolr(solrUrl, crawlDb, linkDb, segments, false, null);
  }

  public void indexSolr(String solrUrl, Path crawlDb, Path linkDb,
          List<Path> segments, boolean noCommit) throws IOException {
    indexSolr(solrUrl, crawlDb, linkDb, segments, noCommit, null);
  }
  
  public void indexSolr(String solrUrl, Path crawlDb, Path linkDb,
      List<Path> segments, boolean noCommit, String solrParams) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("SolrIndexer: starting at " + sdf.format(start));

    final JobConf job = new NutchJob(getConf());
    job.setJobName("index-solr " + solrUrl);

    IndexerMapReduce.initMRJob(crawlDb, linkDb, segments, job);

    job.set(SolrConstants.SERVER_URL, solrUrl);
    if (solrParams != null) {
      job.set(SolrConstants.PARAMS, solrParams);
    }
    NutchIndexWriterFactory.addClassToConf(job, SolrWriter.class);

    job.setReduceSpeculativeExecution(false);

    final Path tmp = new Path("tmp_" + System.currentTimeMillis() + "-" +
                         new Random().nextInt());

    FileOutputFormat.setOutputPath(job, tmp);
    try {
      JobClient.runJob(job);
      // do the commits once and for all the reducers in one go
      SolrServer solr =  SolrUtils.getCommonsHttpSolrServer(job);

      if (!noCommit) {
        solr.commit();
      }
      long end = System.currentTimeMillis();
      LOG.info("SolrIndexer: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
    }
    catch (Exception e){
      LOG.error(e.toString());
    } finally {
      FileSystem.get(job).delete(tmp, true);
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: SolrIndexer <solr url> <crawldb> [-linkdb <linkdb>] [-params k1=v1&k2=v2...] (<segment> ... | -dir <segments>) [-noCommit]");
      return -1;
    }

    final Path crawlDb = new Path(args[1]);
    Path linkDb = null;

    final List<Path> segments = new ArrayList<Path>();
    String params = null;

    boolean noCommit = false;

    for (int i = 2; i < args.length; i++) {
    	if (args[i].equals("-linkdb")) {
    		linkDb = new Path(args[++i]);
    	}
    	else if (args[i].equals("-dir")) {
        Path dir = new Path(args[++i]);
        FileSystem fs = dir.getFileSystem(getConf());
        FileStatus[] fstats = fs.listStatus(dir,
                HadoopFSUtil.getPassDirectoriesFilter(fs));
        Path[] files = HadoopFSUtil.getPaths(fstats);
        for (Path p : files) {
          segments.add(p);
        }
      } else if (args[i].equals("-noCommit")) {
        noCommit = true;
      } else if (args[i].equals("-params")) {
        params = args[++i];
      } else {
        segments.add(new Path(args[i]));
      }
    }

    try {
      indexSolr(args[0], crawlDb, linkDb, segments, noCommit, params);
      return 0;
    } catch (final Exception e) {
      LOG.error("SolrIndexer: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(), new SolrIndexer(), args);
    System.exit(res);
  }
}
