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

import org.apache.nutch.net.*;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/** This class takes a flat file of URLs and adds them to the of pages to be
 * crawled.  Useful for bootstrapping the system. */
public class Injector extends Configured {
  public static final Logger LOG =
    LogFormatter.getLogger("org.apache.nutch.crawl.Injector");


  /** Normalize and filter injected urls. */
  public static class InjectMapper implements Mapper {
    private UrlNormalizer urlNormalizer;
    private float interval;
    private JobConf jobConf;

    public void configure(JobConf job) {
      urlNormalizer = new UrlNormalizerFactory(job).getNormalizer();
      interval = job.getFloat("db.default.fetch.interval", 30f);
      this.jobConf = job;
    }

    public void close() {}

    public void map(WritableComparable key, Writable val,
                    OutputCollector output, Reporter reporter)
      throws IOException {
      UTF8 value = (UTF8)val;
      String url = value.toString();              // value is line of text
      // System.out.println("url: " +url);
      try {
        url = urlNormalizer.normalize(url);       // normalize the url
        URLFilters filters = new URLFilters(this.jobConf);
        url = filters.filter(url);             // filter the url
      } catch (Exception e) {
        LOG.warning("Skipping " +url+":"+e);
        url = null;
      }
      if (url != null) {                          // if it passes
        value.set(url);                           // collect it
        output.collect(value, new CrawlDatum(CrawlDatum.STATUS_DB_UNFETCHED,
                                             interval));
      }
    }
  }

  /** Combine multiple new entries for a url. */
  public static class InjectReducer implements Reducer {
    public void configure(JobConf job) {}
    public void close() {}

    public void reduce(WritableComparable key, Iterator values,
                       OutputCollector output, Reporter reporter)
      throws IOException {
      output.collect(key, (Writable)values.next()); // just collect first value
    }
  }

  /** Construct an Injector. */
  public Injector(Configuration conf) {
    super(conf);
  }

  public void inject(File crawlDb, File urlDir) throws IOException {
    LOG.info("Injector: starting");
    LOG.info("Injector: crawlDb: " + crawlDb);
    LOG.info("Injector: urlDir: " + urlDir);

    File tempDir =
      new File(getConf().get("mapred.temp.dir", ".") +
               "/inject-temp-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    // map text input file to a <url,CrawlDatum> file
    LOG.info("Injector: Converting injected urls to crawl db entries.");
    JobConf sortJob = new NutchJob(getConf());
    sortJob.setInputDir(urlDir);
    sortJob.setMapperClass(InjectMapper.class);
    sortJob.setReducerClass(InjectReducer.class);

    sortJob.setOutputDir(tempDir);
    sortJob.setOutputFormat(SequenceFileOutputFormat.class);
    sortJob.setOutputKeyClass(UTF8.class);
    sortJob.setOutputValueClass(CrawlDatum.class);
    JobClient.runJob(sortJob);

    // merge with existing crawl db
    LOG.info("Injector: Merging injected urls into crawl db.");
    JobConf mergeJob = CrawlDb.createJob(getConf(), crawlDb);
    mergeJob.addInputDir(tempDir);
    JobClient.runJob(mergeJob);
    CrawlDb.install(mergeJob, crawlDb);

    // clean up
    FileSystem fs = new JobClient(getConf()).getFs();
    fs.delete(tempDir);
    LOG.info("Injector: done");

  }

  public static void main(String[] args) throws Exception {
    Injector injector = new Injector(NutchConfiguration.create());
    
    if (args.length < 2) {
      System.err.println("Usage: Injector <crawldb> <url_dir>");
      return;
    }
    
    injector.inject(new File(args[0]), new File(args[1]));
  }

}
