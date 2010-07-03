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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

/**
 * A tools that dumps out the top urls by number of inlinks, number of outlinks,
 * or by score, to a text file. One of the major uses of this tool is to check
 * the top scoring urls of a link analysis program such as LinkRank.
 * 
 * For number of inlinks or number of outlinks the WebGraph program will need to
 * have been run. For link analysis score a program such as LinkRank will need
 * to have been run which updates the NodeDb of the WebGraph.
 */
public class NodeDumper
  extends Configured
  implements Tool {

  public static final Log LOG = LogFactory.getLog(NodeDumper.class);

  private static enum DumpType {
    INLINKS,
    OUTLINKS,
    SCORES
  }

  /**
   * Outputs the top urls sorted in descending order. Depending on the flag set
   * on the command line, the top urls could be for number of inlinks, for
   * number of outlinks, or for link analysis score.
   */
  public static class Sorter
    extends Configured
    implements Mapper<Text, Node, FloatWritable, Text>,
    Reducer<FloatWritable, Text, Text, FloatWritable> {

    private JobConf conf;
    private boolean inlinks = false;
    private boolean outlinks = false;
    private boolean scores = false;
    private long topn = Long.MAX_VALUE;

    /**
     * Configures the job, sets the flag for type of content and the topN number
     * if any.
     */
    public void configure(JobConf conf) {
      this.conf = conf;
      this.inlinks = conf.getBoolean("inlinks", false);
      this.outlinks = conf.getBoolean("outlinks", false);
      this.scores = conf.getBoolean("scores", true);
      this.topn = conf.getLong("topn", Long.MAX_VALUE);
    }

    public void close() {
    }

    /**
     * Outputs the url with the appropriate number of inlinks, outlinks, or for
     * score.
     */
    public void map(Text key, Node node,
      OutputCollector<FloatWritable, Text> output, Reporter reporter)
      throws IOException {

      float number = 0;
      if (inlinks) {
        number = node.getNumInlinks();
      }
      else if (outlinks) {
        number = node.getNumOutlinks();
      }
      else {
        number = node.getInlinkScore();
      }

      // number collected with negative to be descending
      output.collect(new FloatWritable(-number), key);
    }

    /**
     * Flips and collects the url and numeric sort value.
     */
    public void reduce(FloatWritable key, Iterator<Text> values,
      OutputCollector<Text, FloatWritable> output, Reporter reporter)
      throws IOException {

      // take the negative of the negative to get original value, sometimes 0
      // value are a little weird
      float val = key.get();
      FloatWritable number = new FloatWritable(val == 0 ? 0 : -val);
      long numCollected = 0;

      // collect all values, this time with the url as key
      while (values.hasNext() && (numCollected < topn)) {
        Text url = (Text)WritableUtils.clone(values.next(), conf);
        output.collect(url, number);
        numCollected++;
      }
    }
  }

  /**
   * Runs the process to dump the top urls out to a text file.
   * 
   * @param webGraphDb The WebGraph from which to pull values.
   * 
   * @param topN
   * @param output
   * 
   * @throws IOException If an error occurs while dumping the top values.
   */
  public void dumpNodes(Path webGraphDb, DumpType type, long topN, Path output)
    throws IOException {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("NodeDumper: starting at " + sdf.format(start));
    Path nodeDb = new Path(webGraphDb, WebGraph.NODE_DIR);
    Configuration conf = getConf();

    JobConf dumper = new NutchJob(conf);
    dumper.setJobName("NodeDumper: " + webGraphDb);
    FileInputFormat.addInputPath(dumper, nodeDb);
    dumper.setInputFormat(SequenceFileInputFormat.class);
    dumper.setMapperClass(Sorter.class);
    dumper.setReducerClass(Sorter.class);
    dumper.setMapOutputKeyClass(FloatWritable.class);
    dumper.setMapOutputValueClass(Text.class);
    dumper.setOutputKeyClass(Text.class);
    dumper.setOutputValueClass(FloatWritable.class);
    FileOutputFormat.setOutputPath(dumper, output);
    dumper.setOutputFormat(TextOutputFormat.class);
    dumper.setNumReduceTasks(1);
    dumper.setBoolean("inlinks", type == DumpType.INLINKS);
    dumper.setBoolean("outlinks", type == DumpType.OUTLINKS);
    dumper.setBoolean("scores", type == DumpType.SCORES);
    dumper.setLong("topn", topN);

    try {
      LOG.info("NodeDumper: running");
      JobClient.runJob(dumper);
    }
    catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }
    long end = System.currentTimeMillis();
    LOG.info("NodeDumper: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args)
    throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new NodeDumper(),
      args);
    System.exit(res);
  }

  /**
   * Runs the node dumper tool.
   */
  public int run(String[] args)
    throws Exception {

    Options options = new Options();
    Option helpOpts = OptionBuilder.withArgName("help").withDescription(
      "show this help message").create("help");
    Option webGraphDbOpts = OptionBuilder.withArgName("webgraphdb").hasArg().withDescription(
      "the web graph database to use").create("webgraphdb");
    Option inlinkOpts = OptionBuilder.withArgName("inlinks").withDescription(
      "show highest inlinks").create("inlinks");
    Option outlinkOpts = OptionBuilder.withArgName("outlinks").withDescription(
      "show highest outlinks").create("outlinks");
    Option scoreOpts = OptionBuilder.withArgName("scores").withDescription(
      "show highest scores").create("scores");
    Option topNOpts = OptionBuilder.withArgName("topn").hasOptionalArg().withDescription(
      "show topN scores").create("topn");
    Option outputOpts = OptionBuilder.withArgName("output").hasArg().withDescription(
      "the output directory to use").create("output");
    options.addOption(helpOpts);
    options.addOption(webGraphDbOpts);
    options.addOption(inlinkOpts);
    options.addOption(outlinkOpts);
    options.addOption(scoreOpts);
    options.addOption(topNOpts);
    options.addOption(outputOpts);

    CommandLineParser parser = new GnuParser();
    try {

      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("webgraphdb")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("NodeDumper", options);
        return -1;
      }

      String webGraphDb = line.getOptionValue("webgraphdb");
      boolean inlinks = line.hasOption("inlinks");
      boolean outlinks = line.hasOption("outlinks");
      boolean scores = line.hasOption("scores");
      long topN = (line.hasOption("topn")
        ? Long.parseLong(line.getOptionValue("topn")) : Long.MAX_VALUE);

      // get the correct dump type
      String output = line.getOptionValue("output");
      DumpType type = (inlinks ? DumpType.INLINKS : outlinks
        ? DumpType.OUTLINKS : DumpType.SCORES);

      dumpNodes(new Path(webGraphDb), type, topN, new Path(output));
      return 0;
    }
    catch (Exception e) {
      LOG.fatal("NodeDumper: " + StringUtils.stringifyException(e));
      return -2;
    }
  }
}
