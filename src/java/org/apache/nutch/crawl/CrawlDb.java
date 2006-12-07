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

import java.io.*;
import java.util.*;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolBase;

import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/**
 * This class takes the output of the fetcher and updates the
 * crawldb accordingly.
 */
public class CrawlDb extends ToolBase {
  public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";

  public static final Log LOG = LogFactory.getLog(CrawlDb.class);
  
  public CrawlDb() {
    
  }
  
  public CrawlDb(Configuration conf) {
    setConf(conf);
  }

  public void update(Path crawlDb, Path segment, boolean normalize, boolean filter) throws IOException {
    boolean additionsAllowed = getConf().getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);    
    update(crawlDb, segment, normalize, filter, additionsAllowed);
  }
  
  public void update(Path crawlDb, Path segment, boolean normalize, boolean filter, boolean additionsAllowed) throws IOException {
    
    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb update: starting");
      LOG.info("CrawlDb update: db: " + crawlDb);
      LOG.info("CrawlDb update: segment: " + segment);
      LOG.info("CrawlDb update: additions allowed: " + additionsAllowed);
      LOG.info("CrawlDb update: URL normalizing: " + normalize);
      LOG.info("CrawlDb update: URL filtering: " + filter);
    }

    JobConf job = CrawlDb.createJob(getConf(), crawlDb);
    job.setBoolean(CRAWLDB_ADDITIONS_ALLOWED, additionsAllowed);
    job.setBoolean(CrawlDbFilter.URL_FILTERING, filter);
    job.setBoolean(CrawlDbFilter.URL_NORMALIZING, normalize);
    job.addInputPath(new Path(segment, CrawlDatum.FETCH_DIR_NAME));
    job.addInputPath(new Path(segment, CrawlDatum.PARSE_DIR_NAME));

    if (LOG.isInfoEnabled()) {
      LOG.info("CrawlDb update: Merging segment data into db.");
    }
    JobClient.runJob(job);

    CrawlDb.install(job, crawlDb);
    if (LOG.isInfoEnabled()) { LOG.info("CrawlDb update: done"); }
  }

  public static JobConf createJob(Configuration config, Path crawlDb)
    throws IOException {
    Path newCrawlDb =
      new Path(crawlDb,
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("crawldb " + crawlDb);


    Path current = new Path(crawlDb, CrawlDatum.DB_DIR_NAME);
    if (FileSystem.get(job).exists(current)) {
      job.addInputPath(current);
    }
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(CrawlDbFilter.class);
    job.setReducerClass(CrawlDbReducer.class);

    job.setOutputPath(newCrawlDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    return job;
  }

  public static void install(JobConf job, Path crawlDb) throws IOException {
    Path newCrawlDb = job.getOutputPath();
    FileSystem fs = new JobClient(job).getFs();
    Path old = new Path(crawlDb, "old");
    Path current = new Path(crawlDb, CrawlDatum.DB_DIR_NAME);
    if (fs.exists(current)) {
      if (fs.exists(old)) fs.delete(old);
      fs.rename(current, old);
    }
    fs.mkdirs(crawlDb);
    fs.rename(newCrawlDb, current);
    if (fs.exists(old)) fs.delete(old);
  }

  public static void main(String[] args) throws Exception {
    int res = new CrawlDb().doMain(NutchConfiguration.create(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: CrawlDb <crawldb> <segment> [-normalize] [-filter] [-noAdditions]");
      System.err.println("\tcrawldb\tCrawlDb to update");
      System.err.println("\tsegment\tsegment name to update from");
      System.err.println("\t-normalize\tuse URLNormalizer on urls in CrawlDb and segment (usually not needed)");
      System.err.println("\t-filter\tuse URLFilters on urls in CrawlDb and segment");
      System.err.println("\t-noAdditions\tonly update already existing URLs, don't add any newly discovered URLs");
      return -1;
    }
    boolean normalize = false;
    boolean filter = false;
    boolean additionsAllowed = getConf().getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);
    if (args.length > 2) {
      for (int i = 2; i < args.length; i++) {
        if (args[i].equals("-normalize")) {
          normalize = true;
        } else if (args[i].equals("-filter")) {
          filter = true;
        } else if (args[i].equals("-noAdditions")) {
          additionsAllowed = false;
        }
      }
    }
    try {
      update(new Path(args[0]), new Path(args[1]), normalize, filter, additionsAllowed);
      return 0;
    } catch (Exception e) {
      LOG.fatal("CrawlDb update: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
}
