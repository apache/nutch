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
import java.text.SimpleDateFormat;
import java.util.*;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.nutch.net.*;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;

/**
 * This class takes a flat file of URLs and adds them to the of pages to be
 * crawled. Useful for bootstrapping the system. The URL files contain one URL
 * per line, optionally followed by custom metadata separated by tabs with the
 * metadata key separated from the corresponding value by '='. <br>
 * Note that some metadata keys are reserved : <br>
 * - <i>nutch.score</i> : allows to set a custom score for a specific URL <br>
 * - <i>nutch.fetchInterval</i> : allows to set a custom fetch interval for a
 * specific URL <br>
 * - <i>nutch.fetchInterval.fixed</i> : allows to set a custom fetch interval
 * for a specific URL that is not changed by AdaptiveFetchSchedule <br>
 * e.g. http://www.nutch.org/ \t nutch.score=10 \t nutch.fetchInterval=2592000
 * \t userType=open_source
 **/
public class Injector extends NutchTool implements Tool {
  public static final Logger LOG = LoggerFactory.getLogger(Injector.class);

  /** metadata key reserved for setting a custom score for a specific URL */
  public static String nutchScoreMDName = "nutch.score";
  /**
   * metadata key reserved for setting a custom fetchInterval for a specific URL
   */
  public static String nutchFetchIntervalMDName = "nutch.fetchInterval";
  /**
   * metadata key reserved for setting a fixed custom fetchInterval for a
   * specific URL
   */
  public static String nutchFixedFetchIntervalMDName = "nutch.fetchInterval.fixed";

  /** Normalize and filter injected urls. */
  public static class InjectMapper implements
      Mapper<WritableComparable<?>, Text, Text, CrawlDatum> {
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
      curTime = job
          .getLong("injector.current.time", System.currentTimeMillis());
    }

    public void close() {
    }

    public void map(WritableComparable<?> key, Text value,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
      String url = value.toString().trim(); // value is line of text

      if (url != null && (url.length() == 0 || url.startsWith("#"))) {
        /* Ignore line that start with # */
        return;
      }

      // if tabs : metadata that could be stored
      // must be name=value and separated by \t
      float customScore = -1f;
      int customInterval = interval;
      int fixedInterval = -1;
      Map<String, String> metadata = new TreeMap<String, String>();
      if (url.indexOf("\t") != -1) {
        String[] splits = url.split("\t");
        url = splits[0];
        for (int s = 1; s < splits.length; s++) {
          // find separation between name and value
          int indexEquals = splits[s].indexOf("=");
          if (indexEquals == -1) {
            // skip anything without a =
            continue;
          }
          String metaname = splits[s].substring(0, indexEquals);
          String metavalue = splits[s].substring(indexEquals + 1);
          if (metaname.equals(nutchScoreMDName)) {
            try {
              customScore = Float.parseFloat(metavalue);
            } catch (NumberFormatException nfe) {
            }
          } else if (metaname.equals(nutchFetchIntervalMDName)) {
            try {
              customInterval = Integer.parseInt(metavalue);
            } catch (NumberFormatException nfe) {
            }
          } else if (metaname.equals(nutchFixedFetchIntervalMDName)) {
            try {
              fixedInterval = Integer.parseInt(metavalue);
            } catch (NumberFormatException nfe) {
            }
          } else
            metadata.put(metaname, metavalue);
        }
      }
      try {
        url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INJECT);
        url = filters.filter(url); // filter the url
      } catch (Exception e) {
        if (LOG.isWarnEnabled()) {
          LOG.warn("Skipping " + url + ":" + e);
        }
        url = null;
      }
      if (url == null) {
        reporter.getCounter("injector", "urls_filtered").increment(1);
      } else { // if it passes
        value.set(url); // collect it
        CrawlDatum datum = new CrawlDatum();
        datum.setStatus(CrawlDatum.STATUS_INJECTED);

        // Is interval custom? Then set as meta data
        if (fixedInterval > -1) {
          // Set writable using float. Flaot is used by
          // AdaptiveFetchSchedule
          datum.getMetaData().put(Nutch.WRITABLE_FIXED_INTERVAL_KEY,
              new FloatWritable(fixedInterval));
          datum.setFetchInterval(fixedInterval);
        } else {
          datum.setFetchInterval(customInterval);
        }

        datum.setFetchTime(curTime);
        // now add the metadata
        Iterator<String> keysIter = metadata.keySet().iterator();
        while (keysIter.hasNext()) {
          String keymd = keysIter.next();
          String valuemd = metadata.get(keymd);
          datum.getMetaData().put(new Text(keymd), new Text(valuemd));
        }
        if (customScore != -1)
          datum.setScore(customScore);
        else
          datum.setScore(scoreInjected);
        try {
          scfilters.injectedScore(value, datum);
        } catch (ScoringFilterException e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Cannot filter injected score for url " + url
                + ", using default (" + e.getMessage() + ")");
          }
        }
        reporter.getCounter("injector", "urls_injected").increment(1);
        output.collect(value, datum);
      }
    }
  }

  /** Combine multiple new entries for a url. */
  public static class InjectReducer implements
      Reducer<Text, CrawlDatum, Text, CrawlDatum> {
    private int interval;
    private float scoreInjected;
    private boolean overwrite = false;
    private boolean update = false;

    public void configure(JobConf job) {
      interval = job.getInt("db.fetch.interval.default", 2592000);
      scoreInjected = job.getFloat("db.score.injected", 1.0f);
      overwrite = job.getBoolean("db.injector.overwrite", false);
      update = job.getBoolean("db.injector.update", false);
      LOG.info("Injector: overwrite: " + overwrite);
      LOG.info("Injector: update: " + update);
    }

    public void close() {
    }

    private CrawlDatum old = new CrawlDatum();
    private CrawlDatum injected = new CrawlDatum();

    public void reduce(Text key, Iterator<CrawlDatum> values,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
      boolean oldSet = false;
      boolean injectedSet = false;
      while (values.hasNext()) {
        CrawlDatum val = values.next();
        if (val.getStatus() == CrawlDatum.STATUS_INJECTED) {
          injected.set(val);
          injected.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
          injectedSet = true;
        } else {
          old.set(val);
          oldSet = true;
        }

      }

      CrawlDatum res = null;

      // Old default behaviour
      if (injectedSet && !oldSet) {
        res = injected;
      } else {
        res = old;
      }
      if (injectedSet && oldSet) {
        reporter.getCounter("injector", "urls_merged").increment(1);
      }
      /**
       * Whether to overwrite, ignore or update existing records
       * 
       * @see https://issues.apache.org/jira/browse/NUTCH-1405
       */
      // Injected record already exists and update but not overwrite
      if (injectedSet && oldSet && update && !overwrite) {
        res = old;
        old.putAllMetaData(injected);
        old.setScore(injected.getScore() != scoreInjected ? injected.getScore()
            : old.getScore());
        old.setFetchInterval(injected.getFetchInterval() != interval ? injected
            .getFetchInterval() : old.getFetchInterval());
      }

      // Injected record already exists and overwrite
      if (injectedSet && oldSet && overwrite) {
        res = injected;
      }

      output.collect(key, res);
    }
  }

  public Injector() {
  }

  public Injector(Configuration conf) {
    setConf(conf);
  }

  public void inject(Path crawlDb, Path urlDir) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("Injector: starting at " + sdf.format(start));
      LOG.info("Injector: crawlDb: " + crawlDb);
      LOG.info("Injector: urlDir: " + urlDir);
    }

    Path tempDir = new Path(getConf().get("mapred.temp.dir", ".")
        + "/inject-temp-"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    // map text input file to a <url,CrawlDatum> file
    if (LOG.isInfoEnabled()) {
      LOG.info("Injector: Converting injected urls to crawl db entries.");
    }

    FileSystem fs = FileSystem.get(getConf());
    // determine if the crawldb already exists
    boolean dbExists = fs.exists(crawlDb);

    JobConf sortJob = new NutchJob(getConf());
    sortJob.setJobName("inject " + urlDir);
    FileInputFormat.addInputPath(sortJob, urlDir);
    sortJob.setMapperClass(InjectMapper.class);

    FileOutputFormat.setOutputPath(sortJob, tempDir);
    if (dbExists) {
      // Don't run merge injected urls, wait for merge with
      // existing DB
      sortJob.setOutputFormat(SequenceFileOutputFormat.class);
      sortJob.setNumReduceTasks(0);
    } else {
      sortJob.setOutputFormat(MapFileOutputFormat.class);
      sortJob.setReducerClass(InjectReducer.class);
      sortJob.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
          false);
    }
    sortJob.setOutputKeyClass(Text.class);
    sortJob.setOutputValueClass(CrawlDatum.class);
    sortJob.setLong("injector.current.time", System.currentTimeMillis());

    RunningJob mapJob = null;
    try {
      mapJob = JobClient.runJob(sortJob);
    } catch (IOException e) {
      fs.delete(tempDir, true);
      throw e;
    }
    long urlsInjected = mapJob.getCounters()
        .findCounter("injector", "urls_injected").getValue();
    long urlsFiltered = mapJob.getCounters()
        .findCounter("injector", "urls_filtered").getValue();
    LOG.info("Injector: Total number of urls rejected by filters: "
        + urlsFiltered);
    LOG.info("Injector: Total number of urls after normalization: "
        + urlsInjected);
    long urlsMerged = 0;
    if (dbExists) {
      // merge with existing crawl db
      if (LOG.isInfoEnabled()) {
        LOG.info("Injector: Merging injected urls into crawl db.");
      }
      JobConf mergeJob = CrawlDb.createJob(getConf(), crawlDb);
      FileInputFormat.addInputPath(mergeJob, tempDir);
      mergeJob.setReducerClass(InjectReducer.class);
      try {
        RunningJob merge = JobClient.runJob(mergeJob);
        urlsMerged = merge.getCounters().findCounter("injector", "urls_merged")
            .getValue();
        LOG.info("Injector: URLs merged: " + urlsMerged);
      } catch (IOException e) {
        fs.delete(tempDir, true);
        throw e;
      }
      CrawlDb.install(mergeJob, crawlDb);
    } else {
      CrawlDb.install(sortJob, crawlDb);
    }

    // clean up
    fs.delete(tempDir, true);
    LOG.info("Injector: Total new urls injected: "
        + (urlsInjected - urlsMerged));
    long end = System.currentTimeMillis();
    LOG.info("Injector: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
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
      LOG.error("Injector: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  @Override
  /**
   * Used by the Nutch REST service
   */
  public Map<String, Object> run(Map<String, String> args, String crawlId) throws Exception {
    if(args.size()<1){
      throw new IllegalArgumentException("Required arguments <url_dir>");
    }
    Map<String, Object> results = new HashMap<String, Object>();
    String RESULT = "result";
    String crawldb = crawlId+"/crawldb";
    String url_dir = args.get("url_dir");

    inject(new Path(crawldb), new Path(url_dir));
    results.put(RESULT, Integer.toString(0));
    return results;

  }

}
