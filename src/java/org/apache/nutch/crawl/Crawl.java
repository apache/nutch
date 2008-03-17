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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.nutch.fetcher.Fetcher;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.indexer.DeleteDuplicates;
import org.apache.nutch.indexer.IndexMerger;
import org.apache.nutch.indexer.Indexer;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

public class Crawl {
  public static final Log LOG = LogFactory.getLog(Crawl.class);

  private static String getDate() {
    return new SimpleDateFormat("yyyyMMddHHmmss").format
      (new Date(System.currentTimeMillis()));
  }


  /* Perform complete crawling and indexing given a set of root urls. */
  public static void main(String args[]) throws Exception {
    if (args.length < 1) {
      System.out.println
        ("Usage: Crawl <urlDir> [-dir d] [-threads n] [-depth i] [-topN N]");
      return;
    }

    Configuration conf = NutchConfiguration.create();
    conf.addResource("crawl-tool.xml");
    JobConf job = new NutchJob(conf);

    Path rootUrlDir = null;
    Path dir = new Path("crawl-" + getDate());
    int threads = job.getInt("fetcher.threads.fetch", 10);
    int depth = 5;
    long topN = Long.MAX_VALUE;

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
      } else if (args[i] != null) {
        rootUrlDir = new Path(args[i]);
      }
    }

    FileSystem fs = FileSystem.get(job);

    if (LOG.isInfoEnabled()) {
      LOG.info("crawl started in: " + dir);
      LOG.info("rootUrlDir = " + rootUrlDir);
      LOG.info("threads = " + threads);
      LOG.info("depth = " + depth);
      if (topN != Long.MAX_VALUE)
        LOG.info("topN = " + topN);
    }
    
    Path crawlDb = new Path(dir + "/crawldb");
    Path linkDb = new Path(dir + "/linkdb");
    Path segments = new Path(dir + "/segments");
    Path indexes = new Path(dir + "/indexes");
    Path index = new Path(dir + "/index");

    Path tmpDir = job.getLocalPath("crawl"+Path.SEPARATOR+getDate());
    Injector injector = new Injector(conf);
    Generator generator = new Generator(conf);
    Fetcher fetcher = new Fetcher(conf);
    ParseSegment parseSegment = new ParseSegment(conf);
    CrawlDb crawlDbTool = new CrawlDb(conf);
    LinkDb linkDbTool = new LinkDb(conf);
    Indexer indexer = new Indexer(conf);
    DeleteDuplicates dedup = new DeleteDuplicates(conf);
    IndexMerger merger = new IndexMerger(conf);
      
    // initialize crawlDb
    injector.inject(crawlDb, rootUrlDir);
    int i;
    for (i = 0; i < depth; i++) {             // generate new segment
      Path segment = generator.generate(crawlDb, segments, -1, topN, System
          .currentTimeMillis());
      if (segment == null) {
        LOG.info("Stopping at depth=" + i + " - no more URLs to fetch.");
        break;
      }
      fetcher.fetch(segment, threads);  // fetch it
      if (!Fetcher.isParsing(job)) {
        parseSegment.parse(segment);    // parse it, if needed
      }
      crawlDbTool.update(crawlDb, new Path[]{segment}, true, true); // update crawldb
    }
    if (i > 0) {
      linkDbTool.invert(linkDb, segments, true, true, false); // invert links

      // Delete old indexes
      if (fs.exists(indexes)) {
        LOG.info("Deleting old indexes: " + indexes);
        fs.delete(indexes);
      }

      // Delete old index
      if (fs.exists(index)) {
        LOG.info("Deleting old merged index: " + index);
        fs.delete(index);
      }

      // index, dedup & merge
      indexer.index(indexes, crawlDb, linkDb, fs.listPaths(segments, HadoopFSUtil.getPassAllFilter()));
      dedup.dedup(new Path[] { indexes });
      merger.merge(fs.listPaths(indexes, HadoopFSUtil.getPassAllFilter()), index, tmpDir);
    } else {
      LOG.warn("No URLs to fetch - check your seed list and URL filters.");
    }
    if (LOG.isInfoEnabled()) { LOG.info("crawl finished: " + dir); }
  }
}
