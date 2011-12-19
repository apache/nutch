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
package org.apache.nutch.tools;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;

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
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TimingUtil;

/**
 * Dumps all the entries matching a regular expression on their URL. Generates a
 * text representation of the CrawlDatum-s or binary objects which can then be
 * used as a new CrawlDB. The dump mechanism of the crawldb reader is not very
 * useful on large crawldbs as the ouput can be extremely large and the -url
 * function can't help if we don't know what url we want to have a look at.
 *
 * @author : Julien Nioche
 */
public class CrawlDBScanner extends Configured implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(CrawlDBScanner.class);


  static class CrawlDBScannerMapper extends Mapper<Text,CrawlDatum,Text,CrawlDatum> {
    private String regex = null;
    private String status = null;

    public void setup(Context context) {
      regex = context.getConfiguration().get("CrawlDBScanner.regex");
      status = context.getConfiguration().get("CrawlDBScanner.status");
    }

    public void map(Text url, CrawlDatum crawlDatum, Context context) throws IOException, InterruptedException {
      // check status
      if (status != null
          && !status.equalsIgnoreCase(CrawlDatum.getStatusName(crawlDatum.getStatus()))) return;

      // if URL matched regexp dump it
      if (url.toString().matches(regex)) {
        context.write(url, crawlDatum);
      }
    }
  }

  static class CrawlDBScannerReducer extends Reducer <Text,CrawlDatum,Text,CrawlDatum> {
    public void reduce(Text key, Iterable<CrawlDatum> values, Context context) throws IOException, InterruptedException {
      for (CrawlDatum val : values) {
        context.write(key, val);
      }
    }
  }

  private void scan(Path crawlDb, Path outputPath, String regex, String status,
      boolean text) throws Exception {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("CrawlDB scanner: starting at " + sdf.format(start));


    Configuration conf = getConf();
    conf.set("CrawlDBScanner.regex", regex);
    if (status != null) conf.set("CrawlDBScanner.status", status);
    if (text) conf.set("mapred.output.compress", "false");
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

    Job job = new Job(conf, "Scan : " + crawlDb + " for URLS matching : " + regex);
    job.setJarByClass(CrawlDBScanner.class);

    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setMapperClass(CrawlDBScannerMapper.class);
    job.setReducerClass(CrawlDBScannerReducer.class);

    FileOutputFormat.setOutputPath(job, outputPath);

    // if we want a text dump of the entries
    // in order to check something - better to use the text format and avoid
    // compression
    if (text) {
      job.setOutputFormatClass(TextOutputFormat.class);
    }
    // otherwise what we will actually create is a mini-crawlDB which can be
    // then used
    // for debugging
    else {
      job.setOutputFormatClass(MapFileOutputFormat.class);
    }

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(CrawlDatum.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    try {
      job.waitForCompletion(true);
    } catch (IOException e) {
      throw e;
    }

    long end = System.currentTimeMillis();
    LOG.info("CrawlDb scanner: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new CrawlDBScanner(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err
          .println("Usage: CrawlDBScanner <crawldb> <output> <regex> [-s <status>] <-text>");
      return -1;
    }

    boolean text = false;

    Path dbDir = new Path(args[0]);
    Path output = new Path(args[1]);

    String status = null;

    for (int i = 2; i < args.length; i++) {
      if (args[i].equals("-text")) {
        text = true;
      } else if (args[i].equals("-s")) {
        i++;
        status = args[i];
      }
    }

    try {
      scan(dbDir, output, args[2], status, text);
      return 0;
    } catch (Exception e) {
      LOG.error("CrawlDBScanner: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

}
