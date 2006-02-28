/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.logging.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;
import org.apache.hadoop.mapred.*;

import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/** This class takes a flat file of URLs and adds them to the of pages to be
 * crawled.  Useful for bootstrapping the system. */
public class CrawlDb extends Configured {

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.crawl.CrawlDb");

  /** Construct an CrawlDb. */
  public CrawlDb(Configuration conf) {
    super(conf);
  }

  public void update(File crawlDb, File segment) throws IOException {
    LOG.info("CrawlDb update: starting");
    LOG.info("CrawlDb update: db: " + crawlDb);
    LOG.info("CrawlDb update: segment: " + segment);

    JobConf job = CrawlDb.createJob(getConf(), crawlDb);
    job.addInputDir(new File(segment, CrawlDatum.FETCH_DIR_NAME));
    job.addInputDir(new File(segment, CrawlDatum.PARSE_DIR_NAME));

    LOG.info("CrawlDb update: Merging segment data into db.");
    JobClient.runJob(job);

    CrawlDb.install(job, crawlDb);
    LOG.info("CrawlDb update: done");
  }

  public static JobConf createJob(Configuration config, File crawlDb) {
    File newCrawlDb =
      new File(crawlDb,
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);

    job.addInputDir(new File(crawlDb, CrawlDatum.DB_DIR_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(CrawlDatum.class);

    job.setReducerClass(CrawlDbReducer.class);

    job.setOutputDir(newCrawlDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(CrawlDatum.class);

    return job;
  }

  public static void install(JobConf job, File crawlDb) throws IOException {
    File newCrawlDb = job.getOutputDir();
    FileSystem fs = new JobClient(job).getFs();
    File old = new File(crawlDb, "old");
    File current = new File(crawlDb, CrawlDatum.DB_DIR_NAME);
    fs.delete(old);
    fs.rename(current, old);
    fs.rename(newCrawlDb, current);
    fs.delete(old);
  }

  public static boolean doMain(String[] args) throws Exception {
    CrawlDb crawlDb = new CrawlDb(NutchConfiguration.create());
    
    if (args.length < 2) {
      System.err.println("Usage: <crawldb> <segment>");
      return false;
    }
    
    crawlDb.update(new File(args[0]), new File(args[1]));

    return true;
  }

  /**
   * main() wrapper that returns proper exit status
   */
  public static void main(String[] args) {
    Runtime rt = Runtime.getRuntime();
    try {
      boolean status = doMain(args);
      rt.exit(status ? 0 : 1);
    }
    catch (Exception e) {
      LOG.log(Level.SEVERE, "error, caught Exception in main()", e);
      rt.exit(1);
    }
  }
}
