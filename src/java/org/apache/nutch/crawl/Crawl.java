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

import java.util.*;
import java.text.*;

// Commons Logging imports
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.indexer.solr.SolrDeleteDuplicates;
import org.apache.nutch.indexer.solr.SolrIndexer;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

import org.apache.nutch.fetcher.Fetcher;

public class Crawl extends Configured implements Tool {
  public static final Log LOG = LogFactory.getLog(Crawl.class);

  private static String getDate() {
    return new SimpleDateFormat("yyyyMMddHHmmss").format
      (new Date(System.currentTimeMillis()));
  }


  /* Perform complete crawling and indexing (to Solr) given a set of root urls and the -solr
     parameter respectively. More information and Usage parameters can be found below. */
  public static void main(String args[]) throws Exception {
    Configuration conf = NutchConfiguration.create();
    int res = ToolRunner.run(conf, new Crawl(), args);
    System.exit(res);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println
      ("Usage: Crawl <urlDir> -solr <solrURL> [-dir d] [-threads n] [-depth i] [-topN N]");
      return -1;
    }
    Path rootUrlDir = null;
    Path dir = new Path("crawl-" + getDate());
    int threads = getConf().getInt("fetcher.threads.fetch", 10);
    int depth = 5;
    long topN = Long.MAX_VALUE;
    String solrUrl = null;
    
    for (int i = 0; i < args.length; i++) {
      if ("-dir".equals(args[i])) {
        dir = new Path(args[i+1]);
        i++;
      } else if ("-threads".equals(args[i])) {
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
      } else if (args[i] != null) {
        rootUrlDir = new Path(args[i]);
      }
    }
    
    JobConf job = new NutchJob(getConf());

    if (solrUrl == null) {
      LOG.warn("solrUrl is not set, indexing will be skipped...");
    }

    FileSystem fs = FileSystem.get(job);

    if (LOG.isInfoEnabled()) {
      LOG.info("crawl started in: " + dir);
      LOG.info("rootUrlDir = " + rootUrlDir);
      LOG.info("threads = " + threads);
      LOG.info("depth = " + depth);      
      LOG.info("solrUrl=" + solrUrl);
      if (topN != Long.MAX_VALUE)
        LOG.info("topN = " + topN);
    }
    
    Path crawlDb = new Path(dir + "/crawldb");
    Path linkDb = new Path(dir + "/linkdb");
    Path segments = new Path(dir + "/segments");
    Path indexes = new Path(dir + "/indexes");
    Path index = new Path(dir + "/index");

    Path tmpDir = job.getLocalPath("crawl"+Path.SEPARATOR+getDate());
    Injector injector = new Injector(getConf());
    Generator generator = new Generator(getConf());
    Fetcher fetcher = new Fetcher(getConf());
    ParseSegment parseSegment = new ParseSegment(getConf());
    CrawlDb crawlDbTool = new CrawlDb(getConf());
    LinkDb linkDbTool = new LinkDb(getConf());
      
    // initialize crawlDb
    injector.inject(crawlDb, rootUrlDir);
    int i;
    for (i = 0; i < depth; i++) {             // generate new segment
      Path[] segs = generator.generate(crawlDb, segments, -1, topN, System
          .currentTimeMillis());
      if (segs == null) {
        LOG.info("Stopping at depth=" + i + " - no more URLs to fetch.");
        break;
      }
      fetcher.fetch(segs[0], threads, org.apache.nutch.fetcher.Fetcher.isParsing(getConf()));  // fetch it
      if (!Fetcher.isParsing(job)) {
        parseSegment.parse(segs[0]);    // parse it, if needed
      }
      crawlDbTool.update(crawlDb, segs, true, true); // update crawldb
    }
    if (i > 0) {
      linkDbTool.invert(linkDb, segments, true, true, false); // invert links

      if (solrUrl != null) {
        // index, dedup & merge
        FileStatus[] fstats = fs.listStatus(segments, HadoopFSUtil.getPassDirectoriesFilter(fs));
        SolrIndexer indexer = new SolrIndexer(getConf());
        indexer.indexSolr(solrUrl, crawlDb, linkDb, 
          Arrays.asList(HadoopFSUtil.getPaths(fstats)));
        SolrDeleteDuplicates dedup = new SolrDeleteDuplicates();
        dedup.setConf(getConf());
        dedup.dedup(solrUrl);
      }
      
    } else {
      LOG.warn("No URLs to fetch - check your seed list and URL filters.");
    }
    if (LOG.isInfoEnabled()) { LOG.info("crawl finished: " + dir); }
    return 0;
  }


}
