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
import java.text.SimpleDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
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

    public void setup(Mapper<Text, CrawlDatum, ByteWritable, Text>.Context context) {
    }

    public void cleanup() throws IOException {
    }

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
    private static final int NUM_MAX_DELETE_REQUEST = 1000;
    private int numDeletes = 0;
    private int totalDeleted = 0;

    private boolean noCommit = false;

    IndexWriters writers = null;

    public void setup(Reducer<ByteWritable, Text, Text, ByteWritable>.Context context) {
      Configuration conf = context.getConfiguration();
      writers = new IndexWriters(conf);
      try {
        writers.open(conf, "Deletion");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      noCommit = conf.getBoolean("noCommit", false);
    }

    public void cleanup() throws IOException {
      // BUFFERING OF CALLS TO INDEXER SHOULD BE HANDLED AT INDEXER LEVEL
      // if (numDeletes > 0) {
      // LOG.info("CleaningJob: deleting " + numDeletes + " documents");
      // // TODO updateRequest.process(solr);
      // totalDeleted += numDeletes;
      // }

      if (totalDeleted > 0 && !noCommit) {
        writers.commit();
      }

      writers.close();

      LOG.info("CleaningJob: deleted a total of " + totalDeleted + " documents");
    }

    public void reduce(ByteWritable key, Iterable<Text> values,
        Context context) throws IOException {
      for (Text document : values) {
        writers.delete(document.toString());
        totalDeleted++;
        context.getCounter("CleaningJobStatus", "Deleted documents").increment(1);
        // if (numDeletes >= NUM_MAX_DELETE_REQUEST) {
        // LOG.info("CleaningJob: deleting " + numDeletes
        // + " documents");
        // // TODO updateRequest.process(solr);
        // // TODO updateRequest = new UpdateRequest();
        // writers.delete(key.toString());
        // totalDeleted += numDeletes;
        // numDeletes = 0;
        // }
      }
    }
  }

  public void delete(String crawldb, boolean noCommit) 
    throws IOException, InterruptedException, ClassNotFoundException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("CleaningJob: starting at " + sdf.format(start));

    Job job = NutchJob.getInstance(getConf());
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

    job.setJobName("CleaningJob");

    // need to expicitely allow deletions
    conf.setBoolean(IndexerMapReduce.INDEXER_DELETE, true);

    try{
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "CleaningJob did not succeed, job status:"
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        throw new RuntimeException(message);
      }
    } catch (InterruptedException | ClassNotFoundException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }

    long end = System.currentTimeMillis();
    LOG.info("CleaningJob: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  public int run(String[] args) throws IOException {
    if (args.length < 1) {
      String usage = "Usage: CleaningJob <crawldb> [-noCommit]";
      LOG.error("Missing crawldb. " + usage);
      System.err.println(usage);
      IndexWriters writers = new IndexWriters(getConf());
      System.err.println(writers.describe());
      return 1;
    }

    boolean noCommit = false;
    if (args.length == 2 && args[1].equals("-noCommit")) {
      noCommit = true;
    }

    try {
      delete(args[0], noCommit);
    } catch (final Exception e) {
      LOG.error("CleaningJob: " + StringUtils.stringifyException(e));
      System.err.println("ERROR CleaningJob: "
          + StringUtils.stringifyException(e));
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
