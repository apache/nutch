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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
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

public class CrawlDBScanner extends Configured implements Tool,
    Mapper<Text,CrawlDatum,Text,CrawlDatum>, Reducer<Text,CrawlDatum,Text,CrawlDatum> {

  public static final Log LOG = LogFactory.getLog(CrawlDBScanner.class);

  public CrawlDBScanner() {}

  public CrawlDBScanner(Configuration conf) {
    setConf(conf);
  }

  public void close() {}

  private String regex = null;
  private String status = null;

  public void configure(JobConf job) {
    regex = job.get("CrawlDBScanner.regex");
    status = job.get("CrawlDBScanner.status");
  }

  public void map(Text url, CrawlDatum crawlDatum,
      OutputCollector<Text,CrawlDatum> output, Reporter reporter) throws IOException {

    // check status
    if (status != null
        && !status.equalsIgnoreCase(CrawlDatum.getStatusName(crawlDatum.getStatus()))) return;

    // if URL matched regexp dump it
    if (url.toString().matches(regex)) {
      output.collect(url, crawlDatum);
    }
  }

  public void reduce(Text key, Iterator<CrawlDatum> values,
      OutputCollector<Text,CrawlDatum> output, Reporter reporter) throws IOException {
    while (values.hasNext()) {
      CrawlDatum val = values.next();
      output.collect(key, val);
    }
  }

  private void scan(Path crawlDb, Path outputPath, String regex, String status,
      boolean text) throws IOException {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("CrawlDB scanner: starting at " + sdf.format(start));

    JobConf job = new NutchJob(getConf());

    job.setJobName("Scan : " + crawlDb + " for URLS matching : " + regex);

    job.set("CrawlDBScanner.regex", regex);
    if (status != null) job.set("CrawlDBScanner.status", status);

    FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(CrawlDBScanner.class);
    job.setReducerClass(CrawlDBScanner.class);

    FileOutputFormat.setOutputPath(job, outputPath);

    // if we want a text dump of the entries
    // in order to check something - better to use the text format and avoid
    // compression
    if (text) {
      job.set("mapred.output.compress", "false");
      job.setOutputFormat(TextOutputFormat.class);
    }
    // otherwise what we will actually create is a mini-crawlDB which can be
    // then used
    // for debugging
    else {
      job.setOutputFormat(MapFileOutputFormat.class);
    }

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(CrawlDatum.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    try {
      JobClient.runJob(job);
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
      LOG.fatal("CrawlDBScanner: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

}
