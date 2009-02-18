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
package org.apache.nutch.tools.compat;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.webgraph.Node;
import org.apache.nutch.util.FSUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.URLUtil;

/**
 * <p>
 * Significant changes were made to representative url logic used for redirects.
 * This tool will fix representative urls stored in current segments and crawl
 * databases. Any new fetches will use the new representative url logic.
 * </p>
 * 
 * <p>
 * All crawl datums are assumed to be temp url redirects. While this may cause
 * some urls to be incorrectly removed, this tool is a temporary measure to be
 * used until fetches can be rerun. This reduce logic is the same for segments
 * fetch and parse directory as well as for existing crawl databases.
 * </p>
 */
public class ReprUrlFixer
  extends Configured
  implements Tool, Reducer<Text, CrawlDatum, Text, CrawlDatum> {

  public static final Log LOG = LogFactory.getLog(ReprUrlFixer.class);
  private JobConf conf;

  public void configure(JobConf conf) {
    this.conf = conf;
  }

  /**
   * Runs the new ReprUrl logic on all crawldatums.
   */
  public void reduce(Text key, Iterator<CrawlDatum> values,
    OutputCollector<Text, CrawlDatum> output, Reporter reporter)
    throws IOException {

    String url = key.toString();
    Node node = null;
    List<CrawlDatum> datums = new ArrayList<CrawlDatum>();

    // get all crawl datums for a given url key, fetch for instance can have
    // more than one under a given key if there are multiple redirects to a
    // given url
    while (values.hasNext()) {
      CrawlDatum datum = values.next();
      datums.add((CrawlDatum)WritableUtils.clone(datum, conf));
    }

    // apply redirect repr url logic for each datum
    for (CrawlDatum datum : datums) {

      MapWritable metadata = datum.getMetaData();
      Text reprUrl = (Text)metadata.get(Nutch.WRITABLE_REPR_URL_KEY);
      byte status = datum.getStatus();
      boolean isCrawlDb = (CrawlDatum.hasDbStatus(datum));
      boolean segFetched = (status == CrawlDatum.STATUS_FETCH_SUCCESS);

      // only if the crawl datum is from the crawldb or is a successfully
      // fetched page from the segments
      if ((isCrawlDb || segFetched) && reprUrl != null) {

        String src = reprUrl.toString();
        String dest = url;
        URL srcUrl = null;
        URL dstUrl = null;

        // both need to be well formed urls
        try {
          srcUrl = new URL(src);
          dstUrl = new URL(url);
        }
        catch (MalformedURLException e) {
        }

        // if the src and repr urls are the same after the new logic then
        // remove the repr url from the metadata as it is no longer needed
        if (srcUrl != null && dstUrl != null) {
          String reprOut = URLUtil.chooseRepr(src, dest, true);
          if (reprOut.equals(dest)) {
            LOG.info("Removing " + reprOut + " from " + dest);
            metadata.remove(Nutch.WRITABLE_REPR_URL_KEY);
          }
        }
      }

      // collect each datum
      output.collect(key, datum);
    }

  }

  public void close() {
  }

  /**
   * Run the fixer on any crawl database and segments specified.
   */
  public void update(Path crawlDb, Path[] segments)
    throws IOException {

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    // run the crawl database through the repr fixer
    if (crawlDb != null) {

      LOG.info("Running ReprUtilFixer " + crawlDb);
      Path crawlDbCurrent = new Path(crawlDb, CrawlDb.CURRENT_NAME);
      Path newCrawlDb = new Path(crawlDb,
        Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

      JobConf updater = new NutchJob(conf);
      updater.setJobName("ReprUtilFixer: " + crawlDb.toString());
      FileInputFormat.addInputPath(updater, crawlDbCurrent);
      FileOutputFormat.setOutputPath(updater, newCrawlDb);
      updater.setInputFormat(SequenceFileInputFormat.class);
      updater.setReducerClass(ReprUrlFixer.class);
      updater.setOutputKeyClass(Text.class);
      updater.setOutputValueClass(CrawlDatum.class);
      updater.setOutputFormat(MapFileOutputFormat.class);

      try {
        JobClient.runJob(updater);
        LOG.info("Installing new crawldb " + crawlDb);
        CrawlDb.install(updater, crawlDb);
      }
      catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw e;
      }
    }

    // run the segments through the repr fixer, logic will be run on both the
    // crawl_parse and the crawl_fetch directories for every segment specified
    if (segments != null) {

      for (int i = 0; i < segments.length; i++) {

        Path segment = segments[i];
        LOG.info("Running ReprUtilFixer " + segment + " fetch");
        Path segFetch = new Path(segment, CrawlDatum.FETCH_DIR_NAME);
        Path newSegFetch = new Path(segment, CrawlDatum.FETCH_DIR_NAME + "-"
          + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        JobConf fetch = new NutchJob(conf);
        fetch.setJobName("ReprUtilFixer: " + segment.toString());
        FileInputFormat.addInputPath(fetch, segFetch);
        FileOutputFormat.setOutputPath(fetch, newSegFetch);
        fetch.setInputFormat(SequenceFileInputFormat.class);
        fetch.setReducerClass(ReprUrlFixer.class);
        fetch.setOutputKeyClass(Text.class);
        fetch.setOutputValueClass(CrawlDatum.class);
        fetch.setOutputFormat(MapFileOutputFormat.class);

        try {
          JobClient.runJob(fetch);
          LOG.info("Installing new segment fetch directory " + newSegFetch);
          FSUtils.replace(fs, segFetch, newSegFetch, true);
          LOG.info("ReprUrlFixer: finished installing segment fetch directory");
        }
        catch (IOException e) {
          LOG.error(StringUtils.stringifyException(e));
          throw e;
        }

        LOG.info("Running ReprUtilFixer " + segment + " parse");
        Path segParse = new Path(segment, CrawlDatum.PARSE_DIR_NAME);
        Path newSegParse = new Path(segment, CrawlDatum.PARSE_DIR_NAME + "-"
          + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        JobConf parse = new NutchJob(conf);
        parse.setJobName("ReprUtilFixer: " + segment.toString());
        FileInputFormat.addInputPath(parse, segParse);
        FileOutputFormat.setOutputPath(parse, newSegParse);
        parse.setInputFormat(SequenceFileInputFormat.class);
        parse.setReducerClass(ReprUrlFixer.class);
        parse.setOutputKeyClass(Text.class);
        parse.setOutputValueClass(CrawlDatum.class);
        parse.setOutputFormat(MapFileOutputFormat.class);

        try {
          JobClient.runJob(parse);
          LOG.info("Installing new segment parse directry " + newSegParse);
          FSUtils.replace(fs, segParse, newSegParse, true);
          LOG.info("ReprUrlFixer: finished installing segment parse directory");
        }
        catch (IOException e) {
          LOG.error(StringUtils.stringifyException(e));
          throw e;
        }
      }
    }
  }

  /**
   * Runs The ReprUrlFixer.
   */
  public static void main(String[] args)
    throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new ReprUrlFixer(),
      args);
    System.exit(res);
  }

  /**
   * Parse command line options and execute the main update logic.
   */
  public int run(String[] args)
    throws Exception {

    Options options = new Options();
    Option helpOpts = OptionBuilder.withArgName("help").withDescription(
      "show this help message").create("help");
    Option crawlDbOpts = OptionBuilder.withArgName("crawldb").hasArg().withDescription(
      "the crawldb to use").create("crawldb");
    Option segOpts = OptionBuilder.withArgName("segment").hasArgs().withDescription(
      "the segment(s) to use").create("segment");
    options.addOption(helpOpts);
    options.addOption(crawlDbOpts);
    options.addOption(segOpts);

    CommandLineParser parser = new GnuParser();
    try {

      // parse out common line arguments and make sure either a crawldb or a
      // segment are specified
      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help")
        || (!line.hasOption("crawldb") && !line.hasOption("segment"))) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ReprUtilFixer", options);
        return -1;
      }

      // create paths for all of the segments specified, multiple segments may
      // be run at once
      String crawlDb = line.getOptionValue("crawldb");
      String[] segments = line.getOptionValues("segment");
      Path[] segPaths = new Path[segments != null ? segments.length : 0];
      if (segments != null) {
        for (int i = 0; i < segments.length; i++) {
          segPaths[i] = new Path(segments[i]);
        }
      }
      update(new Path(crawlDb), segPaths);
      return 0;
    }
    catch (Exception e) {
      LOG.fatal("ReprUtilFixer: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
}