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

package org.apache.nutch.util;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.MissingOptionException;

/**
 * Extracts some simple crawl completion stats from the crawldb
 *
 * Stats will be sorted by host/domain and will be of the form:
 * 1	www.spitzer.caltech.edu FETCHED
 * 50	www.spitzer.caltech.edu UNFETCHED
 *
 */
public class CrawlCompletionStats extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final int MODE_HOST = 1;
  private static final int MODE_DOMAIN = 2;

  private int mode = 0;

  public int run(String[] args) throws Exception {
    Option helpOpt = new Option("h", "help", false, "Show this message");
    Option inDirs = OptionBuilder
        .withArgName("inputDirs")
        .isRequired()
        .withDescription("Comma separated list of crawl directories (e.g., \"./crawl1,./crawl2\")")
        .hasArgs()
        .create("inputDirs");
    Option outDir = OptionBuilder
        .withArgName("outputDir")
        .isRequired()
        .withDescription("Output directory where results should be dumped")
        .hasArgs()
        .create("outputDir");
    Option modeOpt = OptionBuilder
        .withArgName("mode")
        .isRequired()
        .withDescription("Set statistics gathering mode (by 'host' or by 'domain')")
        .hasArgs()
        .create("mode");
    Option numReducers = OptionBuilder
        .withArgName("numReducers")
        .withDescription("Optional number of reduce jobs to use. Defaults to 1")
        .hasArgs()
        .create("numReducers");

    Options options = new Options();
    options.addOption(helpOpt);
    options.addOption(inDirs);
    options.addOption(outDir);
    options.addOption(modeOpt);
    options.addOption(numReducers);

    CommandLineParser parser = new GnuParser();
    CommandLine cli;

    try {
      cli = parser.parse(options, args);
    } catch (MissingOptionException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("CrawlCompletionStats", options, true);
      return 1;
    }

    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("CrawlCompletionStats", options, true);
      return 1;
    }

    String inputDir = cli.getOptionValue("inputDirs");
    String outputDir = cli.getOptionValue("outputDir");

    int numOfReducers = 1;
    if (cli.hasOption("numReducers")) {
      numOfReducers = Integer.parseInt(args[3]);
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("CrawlCompletionStats: starting at {}", sdf.format(start));

    int mode = 0;
    String jobName = "CrawlCompletionStats";
    if (cli.getOptionValue("mode").equals("host")) {
      jobName = "Host CrawlCompletionStats";
      mode = MODE_HOST;
    } else if (cli.getOptionValue("mode").equals("domain")) {
      jobName = "Domain CrawlCompletionStats";
      mode = MODE_DOMAIN;
    } 

    Configuration conf = getConf();
    conf.setInt("domain.statistics.mode", mode);
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

    Job job = Job.getInstance(conf, jobName);
    job.setJarByClass(CrawlCompletionStats.class);

    String[] inputDirsSpecs = inputDir.split(",");
    for (int i = 0; i < inputDirsSpecs.length; i++) {
      File completeInputPath = new File(new File(inputDirsSpecs[i]), "crawldb/current");
      FileInputFormat.addInputPath(job, new Path(completeInputPath.toString()));
      
    }

    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(outputDir));
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapperClass(CrawlCompletionStatsMapper.class);
    job.setReducerClass(CrawlCompletionStatsReducer.class);
    job.setCombinerClass(CrawlCompletionStatsCombiner.class);
    job.setNumReduceTasks(numOfReducers);

    try {
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = jobName + " job did not succeed, job status: "
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        // throw exception so that calling routine can exit with error
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error(jobName + " job failed");
      throw e;
    }

    long end = System.currentTimeMillis();
    LOG.info("CrawlCompletionStats: finished at {}, elapsed: {}",
      sdf.format(end), TimingUtil.elapsedTime(start, end));
    return 0;
  }

  static class CrawlCompletionStatsMapper extends
      Mapper<Text, CrawlDatum, Text, LongWritable> {
    int mode = 0;

    public void setup(Context context) {
      mode = context.getConfiguration().getInt("domain.statistics.mode", MODE_DOMAIN);
    }

    public void map(Text urlText, CrawlDatum datum, Context context)
        throws IOException, InterruptedException {

      URL url = new URL(urlText.toString());
      String out = "";
      switch (mode) {
        case MODE_HOST:
          out = url.getHost();
          break;
        case MODE_DOMAIN:
          out = URLUtil.getDomainName(url);
          break;
      }

      if (datum.getStatus() == CrawlDatum.STATUS_DB_FETCHED
          || datum.getStatus() == CrawlDatum.STATUS_DB_NOTMODIFIED) {
        context.write(new Text(out + " FETCHED"), new LongWritable(1));
      } else {
        context.write(new Text(out + " UNFETCHED"), new LongWritable(1));
      }
    }
  }

  static class CrawlCompletionStatsReducer extends
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

  public static class CrawlCompletionStatsCombiner extends
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
    ToolRunner.run(NutchConfiguration.create(), new CrawlCompletionStats(), args);
  }
}
