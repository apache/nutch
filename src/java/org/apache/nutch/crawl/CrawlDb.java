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

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.net.*;
import org.apache.nutch.util.*;
import org.apache.nutch.mapred.*;
import org.apache.nutch.parse.*;

/** This class takes a flat file of URLs and adds them to the of pages to be
 * crawled.  Useful for bootstrapping the system. */
public class CrawlDb extends NutchConfigured {

  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.crawl.CrawlDb");

  /** Construct an CrawlDb. */
  public CrawlDb(NutchConf conf) {
    super(conf);
  }

  public void update(File crawlDb, File segment) throws IOException {
    JobConf job = CrawlDb.createJob(getConf(), crawlDb);
    job.addInputDir(new File(segment, CrawlDatum.FETCH_DIR_NAME));
    job.addInputDir(new File(segment, CrawlDatum.PARSE_DIR_NAME));
    JobClient.runJob(job);
    CrawlDb.install(job, crawlDb);
  }

  public static JobConf createJob(NutchConf config, File crawlDb) {
    File newCrawlDb =
      new File(crawlDb,
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new JobConf(config);

    job.setInt("partition.url.by.host.seed", new Random().nextInt());
    job.setPartitionerClass(PartitionUrlByHost.class);

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
    NutchFileSystem fs = new JobClient(job).getFs();
    File old = new File(crawlDb, "old");
    File current = new File(crawlDb, CrawlDatum.DB_DIR_NAME);
    fs.delete(old);
    fs.rename(current, old);
    fs.rename(newCrawlDb, current);
    fs.delete(old);
  }

  public static void main(String[] args) throws Exception {
    CrawlDb crawlDb = new CrawlDb(NutchConf.get());
    
    if (args.length < 2) {
      System.err.println("Usage: <crawldb> <segment>");
      return;
    }
    
    crawlDb.update(new File(args[0]), new File(args[1]));
  }



}
