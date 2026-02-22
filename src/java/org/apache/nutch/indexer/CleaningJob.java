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
package org.apache.nutch.indexer;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.metrics.NutchMetrics;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class scans CrawlDB looking for entries with status DB_GONE (404) or
 * DB_DUPLICATE and sends delete requests to indexers for those documents.
 */

public class CleaningJob implements Tool {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  private Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public static class DBFilter extends
      Mapper<Text, CrawlDatum, ByteWritable, Text> {
    private ByteWritable OUT = new ByteWritable(CrawlDatum.STATUS_DB_GONE);

    @Override
    public void map(Text key, CrawlDatum value,
        Context context) throws IOException, InterruptedException {

      if (value.getStatus() == CrawlDatum.STATUS_DB_GONE
          || value.getStatus() == CrawlDatum.STATUS_DB_DUPLICATE) {
        context.write(OUT, key);
      }
    }
  }

  public static class DeleterReducer extends
      Reducer<ByteWritable, Text, Text, ByteWritable> {
    @SuppressWarnings("unused")
    private static final int NUM_MAX_DELETE_REQUEST = 1000;
    @SuppressWarnings("unused")
    private int numDeletes = 0;
    private int totalDeleted = 0;

    private boolean noCommit = false;

    IndexWriters writers = null;

    // Cached counter reference for performance
    private Counter deletedDocumentsCounter;

    @Override
    public void setup(Reducer<ByteWritable, Text, Text, ByteWritable>.Context context) {
      Configuration conf = context.getConfiguration();
      writers = IndexWriters.get(conf);
      try {
        writers.open(conf, "Deletion");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      noCommit = conf.getBoolean("noCommit", false);
      
      // Initialize cached counter reference
      initCounters(context);
    }

    /**
     * Initialize cached counter references to avoid repeated lookups in hot paths.
     */
    private void initCounters(Context context) {
      deletedDocumentsCounter = context.getCounter(
          NutchMetrics.GROUP_CLEANING, NutchMetrics.CLEANING_DELETED_DOCUMENTS_TOTAL);
    }

    @Override
    public void cleanup(Context context) throws IOException {

      if (totalDeleted > 0 && !noCommit) {
        writers.commit();
      }

      writers.close();

      LOG.info("CleaningJob: deleted a total of {} documents", totalDeleted);
    }

    @Override
    public void reduce(ByteWritable key, Iterable<Text> values,
        Context context) throws IOException {
      for (Text document : values) {
        writers.delete(document.toString());
        totalDeleted++;
        deletedDocumentsCounter.increment(1);
      }
    }
  }

  public void delete(String crawldb, boolean noCommit) 
    throws IOException, InterruptedException, ClassNotFoundException {
    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    LOG.info("CleaningJob: starting");

    Job job = Job.getInstance(getConf(), "Nutch CleaningJob: " + crawldb);
    Configuration conf = job.getConfiguration();

    FileInputFormat.addInputPath(job, new Path(crawldb, CrawlDb.CURRENT_NAME));
    conf.setBoolean("noCommit", noCommit);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setMapOutputKeyClass(ByteWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapperClass(DBFilter.class);
    job.setReducerClass(DeleterReducer.class);
    job.setJarByClass(CleaningJob.class);

    // need to expicitely allow deletions
    conf.setBoolean(IndexerMapReduce.INDEXER_DELETE, true);

    try{
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = NutchJob.getJobFailureLogMessage("CleaningJob", job);
        LOG.error(message);
        throw new RuntimeException(message);
      }
    } catch (InterruptedException | ClassNotFoundException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }

    stopWatch.stop();
    LOG.info("CleaningJob: finished, elapsed: {} ms", stopWatch.getTime(TimeUnit.MILLISECONDS));
  }

  @Override
  public int run(String[] args) throws IOException {
    if (args.length < 1) {
      String usage = "Usage: CleaningJob <crawldb> [-noCommit]";
      LOG.error("Missing crawldb.\n{}", usage);
      return 1;
    }

    boolean noCommit = false;
    if (args.length == 2 && args[1].equals("-noCommit")) {
      noCommit = true;
    }

    try {
      delete(args[0], noCommit);
    } catch (final Exception e) {
      LOG.error("CleaningJob:", e);
      return -1;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(NutchConfiguration.create(), new CleaningJob(),
        args);
    System.exit(result);
  }
}
