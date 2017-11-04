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
import org.apache.nutch.metadata.Nutch;

/**
 * Extracts protocol status code information from the crawl database.
 *
 * ProtocolStatusStatistics will give you information on the count
 * of all status codes encountered on your crawl. This can be useful
 * for checking a number of things.
 *
 * An example output run showing the number of encountered status
 * codes such as 200, 300, and a count of un-fetched record.
 *
 * 38	200
 * 19	301
 * 2	302
 * 665	UNFETCHED
 *
 */
public class ProtocolStatusStatistics extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final Text UNFETCHED_TEXT = new Text("UNFETCHED");

  public static Configuration conf;

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: ProtocolStatistics inputDirs outDir [numOfReducer]");

      System.err.println("\tinputDirs\tComma separated list of crawldb input directories");
      System.err.println("\t\t\tE.g.: crawl/crawldb/");

      System.err.println("\toutDir\t\tOutput directory where results should be dumped");

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
    LOG.info("ProtocolStatistics: starting at " + sdf.format(start));

    String jobName = "ProtocolStatistics";

    conf = getConf();
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

    Job job = Job.getInstance(conf, jobName);
    job.setJarByClass(ProtocolStatusStatistics.class);

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

    job.setMapperClass(ProtocolStatusStatisticsMapper.class);
    job.setReducerClass(ProtocolStatusStatisticsReducer.class);
    job.setCombinerClass(ProtocolStatusStatisticsCombiner.class);
    job.setNumReduceTasks(numOfReducers);

    try {
      boolean success = job.waitForCompletion(true);
      if(!success){
        String message = jobName + " job did not succeed, job status: "
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        // throw exception so that calling routine can exit with error
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error(jobName + " job failed", e);
      throw e;
    }

    long end = System.currentTimeMillis();
    LOG.info("ProtocolStatistics: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
    return 0;
  }

  static class ProtocolStatusStatisticsMapper extends
      Mapper<Text, CrawlDatum, Text, LongWritable> {

    public void map(Text urlText, CrawlDatum datum, Context context)
        throws IOException, InterruptedException {
      if (datum.getMetaData().containsKey(Nutch.PROTOCOL_STATUS_CODE_KEY)) {
        context.write((Text) datum.getMetaData().get(Nutch.PROTOCOL_STATUS_CODE_KEY), new LongWritable(1));
      } else {
        context.write(UNFETCHED_TEXT, new LongWritable(1));
      }
    }
  }

  static class ProtocolStatusStatisticsReducer extends
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

  public static class ProtocolStatusStatisticsCombiner extends
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
    ToolRunner.run(NutchConfiguration.create(), new ProtocolStatusStatistics(), args);
  }

}
