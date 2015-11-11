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

package org.apache.nutch.util.domain;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.URLUtil;

/**
 * Extracts some very basic statistics about domains from the crawldb
 */
public class DomainStatistics extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(DomainStatistics.class);

  private static final Text FETCHED_TEXT = new Text("FETCHED");
  private static final Text NOT_FETCHED_TEXT = new Text("NOT_FETCHED");

  public static enum MyCounter {
    FETCHED, NOT_FETCHED, EMPTY_RESULT
  };

  private static final int MODE_HOST = 1;
  private static final int MODE_DOMAIN = 2;
  private static final int MODE_SUFFIX = 3;
  private static final int MODE_TLD = 4;

  private int mode = 0;

  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: DomainStatistics inputDirs outDir mode [numOfReducer]");

      System.err.println("\tinputDirs\tComma separated list of crawldb input directories");
      System.err.println("\t\t\tE.g.: crawl/crawldb/");

      System.err.println("\toutDir\t\tOutput directory where results should be dumped");

      System.err.println("\tmode\t\tSet statistics gathering mode");
      System.err.println("\t\t\t\thost\tGather statistics by host");
      System.err.println("\t\t\t\tdomain\tGather statistics by domain");
      System.err.println("\t\t\t\tsuffix\tGather statistics by suffix");
      System.err.println("\t\t\t\ttld\tGather statistics by top level directory");

      System.err.println("\t[numOfReducers]\tOptional number of reduce jobs to use. Defaults to 1.");
      
      return 1;
    }
    String inputDir = args[0];
    String outputDir = args[1];
    int numOfReducers = 1;

    if (args.length > 3) {
      numOfReducers = Integer.parseInt(args[3]);
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("DomainStatistics: starting at " + sdf.format(start));

    int mode = 0;
    String jobName = "DomainStatistics";
    if (args[2].equals("host")) {
      jobName = "Host statistics";
      mode = MODE_HOST;
    } else if (args[2].equals("domain")) {
      jobName = "Domain statistics";
      mode = MODE_DOMAIN;
    } else if (args[2].equals("suffix")) {
      jobName = "Suffix statistics";
      mode = MODE_SUFFIX;
    } else if (args[2].equals("tld")) {
      jobName = "TLD statistics";
      mode = MODE_TLD;
    }

    Configuration conf = getConf();
    conf.setInt("domain.statistics.mode", mode);
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

    Job job = Job.getInstance(conf, jobName);
    job.setJarByClass(DomainStatistics.class);

    String[] inputDirsSpecs = inputDir.split(",");
    for (int i = 0; i < inputDirsSpecs.length; i++) {
      File completeInputPath = new File(new File(inputDirsSpecs[i]), "current");
      FileInputFormat.addInputPath(job, new Path(completeInputPath.toString()));
    }

    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(outputDir));
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapperClass(DomainStatisticsMapper.class);
    job.setReducerClass(DomainStatisticsReducer.class);
    job.setCombinerClass(DomainStatisticsCombiner.class);
    job.setNumReduceTasks(numOfReducers);

    try {
      job.waitForCompletion(true);
    } catch (Exception e) {
      throw e;
    }

    long end = System.currentTimeMillis();
    LOG.info("DomainStatistics: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
    return 0;
  }

  static class DomainStatisticsMapper extends
      Mapper<Text, CrawlDatum, Text, LongWritable> {
    int mode = 0;

    public void setup(Context context) {
      mode = context.getConfiguration().getInt("domain.statistics.mode",
          MODE_DOMAIN);
    }

    public void map(Text urlText, CrawlDatum datum, Context context)
        throws IOException, InterruptedException {

      if (datum.getStatus() == CrawlDatum.STATUS_DB_FETCHED
          || datum.getStatus() == CrawlDatum.STATUS_DB_NOTMODIFIED) {

        try {
          URL url = new URL(urlText.toString());
          String out = null;
          switch (mode) {
          case MODE_HOST:
            out = url.getHost();
            break;
          case MODE_DOMAIN:
            out = URLUtil.getDomainName(url);
            break;
          case MODE_SUFFIX:
            out = URLUtil.getDomainSuffix(url).getDomain();
            break;
          case MODE_TLD:
            out = URLUtil.getTopLevelDomainName(url);
            break;
          }
          if (out.trim().equals("")) {
            LOG.info("url : " + url);
            context.getCounter(MyCounter.EMPTY_RESULT).increment(1);
          }

          context.write(new Text(out), new LongWritable(1));
        } catch (Exception ex) {
        }

        context.getCounter(MyCounter.FETCHED).increment(1);
        context.write(FETCHED_TEXT, new LongWritable(1));
      } else {
        context.getCounter(MyCounter.NOT_FETCHED).increment(1);
        context.write(NOT_FETCHED_TEXT, new LongWritable(1));
      }
    }
  }

  static class DomainStatisticsReducer extends
      Reducer<Text, LongWritable, LongWritable, Text> {
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long total = 0;

      for (LongWritable val : values) {
        total += val.get();
      }

      context.write(new LongWritable(total), key);
    }
  }

  public static class DomainStatisticsCombiner extends
      Reducer<Text, LongWritable, Text, LongWritable> {
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long total = 0;

      for (LongWritable val : values) {
        total += val.get();
      }
      context.write(key, new LongWritable(total));
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(NutchConfiguration.create(), new DomainStatistics(), args);
  }

}
