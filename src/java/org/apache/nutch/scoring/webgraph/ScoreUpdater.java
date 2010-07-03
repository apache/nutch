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
package org.apache.nutch.scoring.webgraph;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
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
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

/**
 * Updates the score from the WebGraph node database into the crawl database.
 * Any score that is not in the node database is set to the clear score in the 
 * crawl database.
 */
public class ScoreUpdater
  extends Configured
  implements Tool, Mapper<Text, Writable, Text, ObjectWritable>,
  Reducer<Text, ObjectWritable, Text, CrawlDatum> {

  public static final Log LOG = LogFactory.getLog(ScoreUpdater.class);

  private JobConf conf;
  private float clearScore = 0.0f;

  public void configure(JobConf conf) {
    this.conf = conf;
    clearScore = conf.getFloat("link.score.updater.clear.score", 0.0f);
  }

  /**
   * Changes input into ObjectWritables.
   */
  public void map(Text key, Writable value,
    OutputCollector<Text, ObjectWritable> output, Reporter reporter)
    throws IOException {

    ObjectWritable objWrite = new ObjectWritable();
    objWrite.set(value);
    output.collect(key, objWrite);
  }

  /**
   * Creates new CrawlDatum objects with the updated score from the NodeDb or
   * with a cleared score.
   */
  public void reduce(Text key, Iterator<ObjectWritable> values,
    OutputCollector<Text, CrawlDatum> output, Reporter reporter)
    throws IOException {

    String url = key.toString();
    Node node = null;
    CrawlDatum datum = null;

    // set the node and the crawl datum, should be one of each unless no node
    // for url in the crawldb
    while (values.hasNext()) {
      ObjectWritable next = values.next();
      Object value = next.get();
      if (value instanceof Node) {
        node = (Node)value;
      }
      else if (value instanceof CrawlDatum) {
        datum = (CrawlDatum)value;
      }
    }

    // datum should never be null, could happen if somehow the url was 
    // normalized or changed after being pulled from the crawldb
    if (datum != null) {

      if (node != null) {
        
        // set the inlink score in the nodedb
        float inlinkScore = node.getInlinkScore();
        datum.setScore(inlinkScore);
        LOG.debug(url + ": setting to score " + inlinkScore);
      }
      else {
        
        // clear out the score in the crawldb
        datum.setScore(clearScore);
        LOG.debug(url + ": setting to clear score of " + clearScore);
      }

      output.collect(key, datum);
    }
    else {
      LOG.debug(url + ": no datum");
    }
  }

  public void close() {
  }

  /**
   * Updates the inlink score in the web graph node databsae into the crawl 
   * database.
   * 
   * @param crawlDb The crawl database to update
   * @param webGraphDb The webgraph database to use.
   * 
   * @throws IOException If an error occurs while updating the scores.
   */
  public void update(Path crawlDb, Path webGraphDb)
    throws IOException {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("ScoreUpdater: starting at " + sdf.format(start));

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    // create a temporary crawldb with the new scores
    LOG.info("Running crawldb update " + crawlDb);
    Path nodeDb = new Path(webGraphDb, WebGraph.NODE_DIR);
    Path crawlDbCurrent = new Path(crawlDb, CrawlDb.CURRENT_NAME);
    Path newCrawlDb = new Path(crawlDb,
      Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    // run the updater job outputting to the temp crawl database
    JobConf updater = new NutchJob(conf);
    updater.setJobName("Update CrawlDb from WebGraph");
    FileInputFormat.addInputPath(updater, crawlDbCurrent);
    FileInputFormat.addInputPath(updater, nodeDb);
    FileOutputFormat.setOutputPath(updater, newCrawlDb);
    updater.setInputFormat(SequenceFileInputFormat.class);
    updater.setMapperClass(ScoreUpdater.class);
    updater.setReducerClass(ScoreUpdater.class);
    updater.setMapOutputKeyClass(Text.class);
    updater.setMapOutputValueClass(ObjectWritable.class);
    updater.setOutputKeyClass(Text.class);
    updater.setOutputValueClass(CrawlDatum.class);
    updater.setOutputFormat(MapFileOutputFormat.class);

    try {
      JobClient.runJob(updater);
    }
    catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      
      // remove the temp crawldb on error
      if (fs.exists(newCrawlDb)) {
        fs.delete(newCrawlDb, true);
      }
      throw e;
    }

    // install the temp crawl database
    LOG.info("ScoreUpdater: installing new crawldb " + crawlDb);
    CrawlDb.install(updater, crawlDb);

    long end = System.currentTimeMillis();
    LOG.info("ScoreUpdater: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args)
    throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new ScoreUpdater(),
      args);
    System.exit(res);
  }

  /**
   * Runs the ScoreUpdater tool.
   */
  public int run(String[] args)
    throws Exception {

    Options options = new Options();
    Option helpOpts = OptionBuilder.withArgName("help").withDescription(
      "show this help message").create("help");
    Option crawlDbOpts = OptionBuilder.withArgName("crawldb").hasArg().withDescription(
      "the crawldb to use").create("crawldb");
    Option webGraphOpts = OptionBuilder.withArgName("webgraphdb").hasArg().withDescription(
      "the webgraphdb to use").create("webgraphdb");
    options.addOption(helpOpts);
    options.addOption(crawlDbOpts);
    options.addOption(webGraphOpts);

    CommandLineParser parser = new GnuParser();
    try {

      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("webgraphdb")
        || !line.hasOption("crawldb")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ScoreUpdater", options);
        return -1;
      }

      String crawlDb = line.getOptionValue("crawldb");
      String webGraphDb = line.getOptionValue("webgraphdb");
      update(new Path(crawlDb), new Path(webGraphDb));
      return 0;
    }
    catch (Exception e) {
      LOG.fatal("ScoreUpdater: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
}
