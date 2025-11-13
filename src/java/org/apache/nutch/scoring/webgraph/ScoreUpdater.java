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
import java.lang.invoke.MethodHandles;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/**
 * Updates the score from the WebGraph node database into the crawl database.
 * Any score that is not in the node database is set to the clear score in the
 * crawl database.
 */
public class ScoreUpdater extends Configured implements Tool{

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Changes input into ObjectWritables.
   */
  public static class ScoreUpdaterMapper extends
      Mapper<Text, Writable, Text, ObjectWritable> {

    @Override
    public void map(Text key, Writable value,
        Context context)
        throws IOException, InterruptedException {

      ObjectWritable objWrite = new ObjectWritable();
      objWrite.set(value);
      context.write(key, objWrite);
    }
  }

  /**
   * Creates new CrawlDatum objects with the updated score from the NodeDb or
   * with a cleared score.
   */
  public static class ScoreUpdaterReducer extends 
      Reducer<Text, ObjectWritable, Text, CrawlDatum> {
    private float clearScore = 0.0f;

    @Override
    public void setup(Reducer<Text, ObjectWritable, Text, CrawlDatum>.Context context) {
      Configuration conf = context.getConfiguration();
      clearScore = conf.getFloat("link.score.updater.clear.score", 0.0f);
    }

    @Override
    public void reduce(Text key, Iterable<ObjectWritable> values,
        Context context)
        throws IOException, InterruptedException {

      String url = key.toString();
      Node node = null;
      CrawlDatum datum = null;

      // set the node and the crawl datum, should be one of each unless no node
      // for url in the crawldb
      for (ObjectWritable next : values) {
        Object value = next.get();
        if (value instanceof Node) {
          node = (Node) value;
        } else if (value instanceof CrawlDatum) {
          datum = (CrawlDatum) value;
        }
      }

      // datum should never be null, could happen if somehow the url was
      // normalized or changed after being pulled from the crawldb
      if (datum != null) {

        if (node != null) {

          // set the inlink score in the nodedb
          float inlinkScore = node.getInlinkScore();
          datum.setScore(inlinkScore);
          LOG.debug("{}: setting to score {}", url, inlinkScore);
        } else {

          // clear out the score in the crawldb
          datum.setScore(clearScore);
          LOG.debug("{}: setting to clear score of {}", url, clearScore);
        }

        context.write(key, datum);
      } else {
        LOG.debug("{}: no datum", url);
      }
    }
  }


  /**
   * Updates the inlink score in the web graph node databsae into the crawl
   * database.
   * 
   * @param crawlDb
   *          The crawl database to update
   * @param webGraphDb
   *          The webgraph database to use.
   * @throws IOException
   *           If an error occurs while updating the scores.
   * @throws InterruptedException if the Job is interrupted during execution
   * @throws ClassNotFoundException if classes required to run 
   * the Job cannot be located
   */
  public void update(Path crawlDb, Path webGraphDb) throws IOException,
      ClassNotFoundException, InterruptedException {

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();
    LOG.info("ScoreUpdater: starting");

    Configuration conf = getConf();

    // create a temporary crawldb with the new scores
    LOG.info("Running crawldb update {}", crawlDb);
    Path nodeDb = new Path(webGraphDb, WebGraph.NODE_DIR);
    Path crawlDbCurrent = new Path(crawlDb, CrawlDb.CURRENT_NAME);
    Path newCrawlDb = new Path(crawlDb, Integer.toString(new Random()
        .nextInt(Integer.MAX_VALUE)));

    // run the updater job outputting to the temp crawl database
    Job updater = Job.getInstance(conf, "Nutch ScoreUpdater: " + crawlDb);
    FileInputFormat.addInputPath(updater, crawlDbCurrent);
    FileInputFormat.addInputPath(updater, nodeDb);
    FileOutputFormat.setOutputPath(updater, newCrawlDb);
    updater.setInputFormatClass(SequenceFileInputFormat.class);
    updater.setJarByClass(ScoreUpdater.class);
    updater.setMapperClass(ScoreUpdater.ScoreUpdaterMapper.class);
    updater.setReducerClass(ScoreUpdater.ScoreUpdaterReducer.class);
    updater.setMapOutputKeyClass(Text.class);
    updater.setMapOutputValueClass(ObjectWritable.class);
    updater.setOutputKeyClass(Text.class);
    updater.setOutputValueClass(CrawlDatum.class);
    updater.setOutputFormatClass(MapFileOutputFormat.class);

    try {
      boolean success = updater.waitForCompletion(true);
      if (!success) {
        String message = NutchJob
            .getJobFailureLogMessage("Update CrawlDb from WebGraph", updater);
        LOG.error(message);
        // remove the temp crawldb on error
        FileSystem fs = newCrawlDb.getFileSystem(conf);
        if (fs.exists(newCrawlDb)) {
          fs.delete(newCrawlDb, true);
        }
        throw new RuntimeException(message);
      }
    } catch (IOException | ClassNotFoundException | InterruptedException e) {
      LOG.error("Update CrawlDb from WebGraph:", e);

      // remove the temp crawldb on error
      FileSystem fs = newCrawlDb.getFileSystem(conf);
      if (fs.exists(newCrawlDb)) {
        fs.delete(newCrawlDb, true);
      }
      throw e;
    }

    // install the temp crawl database
    LOG.info("ScoreUpdater: installing new crawldb {}", crawlDb);
    CrawlDb.install(updater, crawlDb);

    stopWatch.stop();
    LOG.info("ScoreUpdater: finished, elapsed: {} ms ", stopWatch.getTime(
        TimeUnit.MILLISECONDS));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new ScoreUpdater(),
        args);
    System.exit(res);
  }

  /**
   * Runs the ScoreUpdater tool.
   */
  @Override
  public int run(String[] args) throws Exception {

    Options options = new Options();
    Option helpOpts = Option.builder("help")
        .argName("help")
        .desc("show this help message")
        .build();
    options.addOption(helpOpts);

    Option crawlDbOpts = Option.builder("crawldb")
        .argName("crawldb")
        .hasArg()
        .desc("the crawldb to use")
        .build();
    options.addOption(crawlDbOpts);

    Option webGraphOpts = Option.builder("webgraphdb")
        .argName("webgraphdb")
        .hasArg()
        .desc("the webgraphdb to use")
        .build();
    options.addOption(webGraphOpts);

    CommandLineParser parser = new DefaultParser();
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
    } catch (Exception e) {
      LOG.error("ScoreUpdater:", e);
      return -1;
    }
  }
}
