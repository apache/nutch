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

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;

import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/**
 * This class takes the output of the fetcher and updates the
 * crawldb accordingly.
 */
public class CrawlDb extends Configured {

  public static final Log LOG = LogFactory.getLog(CrawlDb.class);

  /** Construct an CrawlDb. */
  public CrawlDb(Configuration conf) {
    super(conf);
  }

  public void update(Path crawlDb, Path segment) throws IOException {
    LOG.info("CrawlDb update: starting");
    LOG.info("CrawlDb update: db: " + crawlDb);
    LOG.info("CrawlDb update: segment: " + segment);

    JobConf job = CrawlDb.createJob(getConf(), crawlDb);
    job.addInputPath(new Path(segment, CrawlDatum.FETCH_DIR_NAME));
    job.addInputPath(new Path(segment, CrawlDatum.PARSE_DIR_NAME));

    LOG.info("CrawlDb update: Merging segment data into db.");
    JobClient.runJob(job);

    CrawlDb.install(job, crawlDb);
    LOG.info("CrawlDb update: done");
  }

  public static JobConf createJob(Configuration config, Path crawlDb) {
    Path newCrawlDb =
      new Path(crawlDb,
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("crawldb " + crawlDb);

    job.addInputPath(new Path(crawlDb, CrawlDatum.DB_DIR_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(CrawlDatum.class);

    job.setReducerClass(CrawlDbReducer.class);

    job.setOutputPath(newCrawlDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(CrawlDatum.class);

    return job;
  }

  public static void install(JobConf job, Path crawlDb) throws IOException {
    Path newCrawlDb = job.getOutputPath();
    FileSystem fs = new JobClient(job).getFs();
    Path old = new Path(crawlDb, "old");
    Path current = new Path(crawlDb, CrawlDatum.DB_DIR_NAME);
    fs.delete(old);
    fs.rename(current, old);
    fs.mkdirs(crawlDb);
    fs.rename(newCrawlDb, current);
    fs.delete(old);
  }

  public static void main(String[] args) throws Exception {
    CrawlDb crawlDb = new CrawlDb(NutchConfiguration.create());
    
    if (args.length < 2) {
      System.err.println("Usage: <crawldb> <segment>");
      return;
    }
    
    crawlDb.update(new Path(args[0]), new Path(args[1]));
  }



}
