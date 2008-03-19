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
import org.apache.hadoop.util.*;

import org.apache.nutch.net.*;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/** This class takes a flat file of URLs and adds them to the of pages to be
 * crawled.  Useful for bootstrapping the system. */
public class Injector extends Configured implements Tool {
  public static final Log LOG = LogFactory.getLog(Injector.class);


  /** Normalize and filter injected urls. */
  public static class InjectMapper implements Mapper<WritableComparable, Text, Text, CrawlDatum> {
    private URLNormalizers urlNormalizers;
    private int interval;
    private float scoreInjected;
    private JobConf jobConf;
    private URLFilters filters;
    private ScoringFilters scfilters;
    private long curTime;

    public void configure(JobConf job) {
      this.jobConf = job;
      urlNormalizers = new URLNormalizers(job, URLNormalizers.SCOPE_INJECT);
      interval = jobConf.getInt("db.fetch.interval.default", 2592000);
      filters = new URLFilters(jobConf);
      scfilters = new ScoringFilters(jobConf);
      scoreInjected = jobConf.getFloat("db.score.injected", 1.0f);
      curTime = job.getLong("injector.current.time", System.currentTimeMillis());
    }

    public void close() {}

    public void map(WritableComparable key, Text value,
                    OutputCollector<Text, CrawlDatum> output, Reporter reporter)
      throws IOException {
      String url = value.toString();              // value is line of text
      try {
        url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INJECT);
        url = filters.filter(url);             // filter the url
      } catch (Exception e) {
        if (LOG.isWarnEnabled()) { LOG.warn("Skipping " +url+":"+e); }
        url = null;
      }
      if (url != null) {                          // if it passes
        value.set(url);                           // collect it
        CrawlDatum datum = new CrawlDatum(CrawlDatum.STATUS_INJECTED, interval);
        datum.setFetchTime(curTime);
        datum.setScore(scoreInjected);
        try {
          scfilters.injectedScore(value, datum);
        } catch (ScoringFilterException e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Cannot filter injected score for url " + url +
                     ", using default (" + e.getMessage() + ")");
          }
          datum.setScore(scoreInjected);
        }
        output.collect(value, datum);
      }
    }
  }

  /** Combine multiple new entries for a url. */
  public static class InjectReducer implements Reducer<Text, CrawlDatum, Text, CrawlDatum> {
    public void configure(JobConf job) {}    
    public void close() {}

    public void reduce(Text key, Iterator<CrawlDatum> values,
                       OutputCollector<Text, CrawlDatum> output, Reporter reporter)
      throws IOException {
      CrawlDatum old = null;
      CrawlDatum injected = null;
      while (values.hasNext()) {
        CrawlDatum val = values.next();
        if (val.getStatus() == CrawlDatum.STATUS_INJECTED) {
          injected = val;
          injected.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
        } else {
          old = val;
        }
      }
      CrawlDatum res = null;
      if (old != null) res = old; // don't overwrite existing value
      else res = injected;

      output.collect(key, res);
    }
  }

  public Injector() {}
  
  public Injector(Configuration conf) {
    setConf(conf);
  }
  
  public void inject(Path crawlDb, Path urlDir) throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("Injector: starting");
      LOG.info("Injector: crawlDb: " + crawlDb);
      LOG.info("Injector: urlDir: " + urlDir);
    }

    Path tempDir =
      new Path(getConf().get("mapred.temp.dir", ".") +
               "/inject-temp-"+
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    // map text input file to a <url,CrawlDatum> file
    if (LOG.isInfoEnabled()) {
      LOG.info("Injector: Converting injected urls to crawl db entries.");
    }
    JobConf sortJob = new NutchJob(getConf());
    sortJob.setJobName("inject " + urlDir);
    sortJob.setInputPath(urlDir);
    sortJob.setMapperClass(InjectMapper.class);

    sortJob.setOutputPath(tempDir);
    sortJob.setOutputFormat(SequenceFileOutputFormat.class);
    sortJob.setOutputKeyClass(Text.class);
    sortJob.setOutputValueClass(CrawlDatum.class);
    sortJob.setLong("injector.current.time", System.currentTimeMillis());
    JobClient.runJob(sortJob);

    // merge with existing crawl db
    if (LOG.isInfoEnabled()) {
      LOG.info("Injector: Merging injected urls into crawl db.");
    }
    JobConf mergeJob = CrawlDb.createJob(getConf(), crawlDb);
    mergeJob.addInputPath(tempDir);
    mergeJob.setReducerClass(InjectReducer.class);
    JobClient.runJob(mergeJob);
    CrawlDb.install(mergeJob, crawlDb);

    // clean up
    FileSystem fs = FileSystem.get(getConf());
    fs.delete(tempDir);
    if (LOG.isInfoEnabled()) { LOG.info("Injector: done"); }

  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new Injector(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: Injector <crawldb> <url_dir>");
      return -1;
    }
    try {
      inject(new Path(args[0]), new Path(args[1]));
      return 0;
    } catch (Exception e) {
      LOG.fatal("Injector: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

}
