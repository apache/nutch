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
package org.apache.nutch.util.hostdb;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TimingUtil;

/**
 * A utility to dump the contents of HostDB.
 */
public class DumpHostDb extends Configured implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(DumpHostDb.class);

  public static final String HOSTDB_FAILURE_THRESHOLD = "hostdb.failure.threshold";
  public static final String HOSTDB_NUM_PAGES_THRESHOLD = "hostdb.num.pages.threshold";
  public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  static class DumpHostDbMapper extends Mapper<Text, HostDatum, Text, HostDatum> {
    protected Integer failureThreshold = -1;
    protected Integer numPagesThreshold = -1;

    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      failureThreshold = conf.getInt(HOSTDB_FAILURE_THRESHOLD, -1);
      numPagesThreshold = conf.getInt(HOSTDB_NUM_PAGES_THRESHOLD, -1);
    }

    public void map(Text key, HostDatum datum, Context context)
        throws IOException, InterruptedException {
      boolean filter = false;

      if (numPagesThreshold != -1 && (datum.getStat(CrawlDatum.STATUS_DB_FETCHED) +
          datum.getStat(CrawlDatum.STATUS_DB_NOTMODIFIED)) < numPagesThreshold)
        filter = true;
      if (failureThreshold != -1 && datum.numFailures() < numPagesThreshold)
        filter = true;

      if(!filter)
        context.write(key, datum);
    }
  }

  private void dumpHostDb(Path hostDb, Path output, Integer failureThreshold,
                          Integer numPagesThreshold) throws Exception {

    long start = System.currentTimeMillis();
    LOG.info("HostDb dump: starting at " + sdf.format(start));

    Configuration conf = getConf();
    conf.setInt(HOSTDB_FAILURE_THRESHOLD, failureThreshold);
    conf.setInt(HOSTDB_NUM_PAGES_THRESHOLD, numPagesThreshold);
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
    Job job = new Job(conf, "DumpHostDb");
    job.setJarByClass(DumpHostDb.class);

    FileInputFormat.addInputPath(job, new Path(hostDb, "current"));
    FileOutputFormat.setOutputPath(job, output);

    job.setMapperClass(DumpHostDbMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(HostDatum.class);

    try {
      job.waitForCompletion(true);
    } catch (Exception e) {
      LOG.info("Caught exception " + StringUtils.stringifyException(e));
      throw e;
    }

    long end = System.currentTimeMillis();
    LOG.info("HostDb dump: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new DumpHostDb(), args);
    System.exit(res);
  }

  public static void usage() {
    System.err.println("Usage: DumpHostDb <hostdb> <output> [-numPagesThreshold <threshold>] [-dumpFailedHosts <threshold>]");
    System.err.println("\t<hostdb>\tdirectory name where hostdb is located");
    System.err.println("\t<output>\toutput location where the dump will be produced");
    System.err.println("\n Optional arguments:");
    System.err.println("\t[-dumpFailedHosts <threshold>]\tlist status sorted by host");
    System.err.println("\t[-numPagesThreshold <threshold>]\tthreshold for fetched pages of the hosts");
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      usage();
      return -1;
    }

    Path hostdb = new Path(args[0]);
    Path output = new Path(args[1]);

    Integer failureThreshold = -1;
    Integer numPagesThreshold = -1;

    for (int i = 2; i < args.length; i++) {
      if (args[i].equals("-dumpFailedHosts")) {
        failureThreshold = Integer.parseInt(args[++i]);
        LOG.info("HostDb dump: dumping failed hosts with a threshold of " + failureThreshold);
      }
      else if (args[i].equals("-numPagesThreshold")) {
        numPagesThreshold = Integer.parseInt(args[++i]);
        LOG.info("HostDb dump: dumping hosts with page threshold of " + numPagesThreshold );
      }
      else {
        System.err.println("HostDb dump: Found invalid argument : \"" + args[i] + "\"\n");
        usage();
        return -1;
      }
    }

    try {
      dumpHostDb(hostdb, output, failureThreshold, numPagesThreshold);
      return 0;
    } catch (Exception e) {
      LOG.error("HostDb dump: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
}
