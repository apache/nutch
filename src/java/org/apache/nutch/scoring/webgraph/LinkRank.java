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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.FSUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.URLUtil;

public class LinkRank extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  private static final String NUM_NODES = "_num_nodes_";

  /**
   * Runs the counter job. The counter job determines the number of links in the
   * webgraph. This is used during analysis.
   * 
   * @param fs
   *          The job file system.
   * @param webGraphDb
   *          The web graph database to use.
   * 
   * @return The number of nodes in the web graph.
   * @throws IOException
   *           If an error occurs while running the counter job.
   */
  private int runCounter(FileSystem fs, Path webGraphDb) throws IOException,
      ClassNotFoundException, InterruptedException {

    // configure the counter job
    Path numLinksPath = new Path(webGraphDb, NUM_NODES);
    Path nodeDb = new Path(webGraphDb, WebGraph.NODE_DIR);
    Job counter = NutchJob.getInstance(getConf());
    Configuration conf = counter.getConfiguration();
    counter.setJobName("LinkRank Counter");
    FileInputFormat.addInputPath(counter, nodeDb);
    FileOutputFormat.setOutputPath(counter, numLinksPath);
    counter.setInputFormatClass(SequenceFileInputFormat.class);
    counter.setJarByClass(Counter.class);
    counter.setMapperClass(Counter.CountMapper.class);
    counter.setCombinerClass(Counter.CountReducer.class);
    counter.setReducerClass(Counter.CountReducer.class);
    counter.setMapOutputKeyClass(Text.class);
    counter.setMapOutputValueClass(LongWritable.class);
    counter.setOutputKeyClass(Text.class);
    counter.setOutputValueClass(LongWritable.class);
    counter.setNumReduceTasks(1);
    counter.setOutputFormatClass(TextOutputFormat.class);
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
        false);

    // run the counter job, outputs to a single reduce task and file
    LOG.info("Starting link counter job");
    try {
      boolean success = counter.waitForCompletion(true);
      if (!success) {
        String message = "Link counter job did not succeed, job status:"
            + counter.getStatus().getState() + ", reason: "
            + counter.getStatus().getFailureInfo();
        LOG.error(message);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("Link counter job failed:", e);
      throw e;
    }
    
    LOG.info("Finished link counter job");

    // read the first (and only) line from the file which should be the
    // number of links in the web graph
    LOG.info("Reading numlinks temp file");
    FSDataInputStream readLinks = fs.open(new Path(numLinksPath, "part-r-00000"));
    BufferedReader buffer = new BufferedReader(new InputStreamReader(readLinks));
    String numLinksLine = buffer.readLine();
    readLinks.close();

    // check if there are links to process, if none, webgraph might be empty
    if (numLinksLine == null || numLinksLine.length() == 0) {
      fs.delete(numLinksPath, true);
      throw new IOException("No links to process, is the webgraph empty?");
    }

    // delete temp file and convert and return the number of links as an int
    LOG.info("Deleting numlinks temp file");
    fs.delete(numLinksPath, true);
    String numLinks = numLinksLine.split("\\s+")[1];
    return Integer.parseInt(numLinks);
  }

  /**
   * Runs the initializer job. The initializer job sets up the nodes with a
   * default starting score for link analysis.
   * 
   * @param nodeDb
   *          The node database to use.
   * @param output
   *          The job output directory.
   * 
   * @throws IOException
   *           If an error occurs while running the initializer job.
   */
  private void runInitializer(Path nodeDb, Path output) throws IOException,
     InterruptedException, ClassNotFoundException {

    // configure the initializer
    Job initializer = NutchJob.getInstance(getConf());
    Configuration conf = initializer.getConfiguration();
    initializer.setJobName("LinkAnalysis Initializer");
    FileInputFormat.addInputPath(initializer, nodeDb);
    FileOutputFormat.setOutputPath(initializer, output);
    initializer.setJarByClass(Initializer.class);
    initializer.setInputFormatClass(SequenceFileInputFormat.class);
    initializer.setMapperClass(Initializer.class);
    initializer.setMapOutputKeyClass(Text.class);
    initializer.setMapOutputValueClass(Node.class);
    initializer.setOutputKeyClass(Text.class);
    initializer.setOutputValueClass(Node.class);
    initializer.setOutputFormatClass(MapFileOutputFormat.class);
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
        false);

    // run the initializer
    LOG.info("Starting initialization job");
    try {
      boolean success = initializer.waitForCompletion(true);
      if (!success) {
        String message = "Initialization job did not succeed, job status:"
            + initializer.getStatus().getState() + ", reason: "
            + initializer.getStatus().getFailureInfo();
        LOG.error(message);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("Initialization job failed:", e);
      throw e;
    }
    LOG.info("Finished initialization job.");
  }

  /**
   * Runs the inverter job. The inverter job flips outlinks to inlinks to be
   * passed into the analysis job.
   * 
   * @param nodeDb
   *          The node database to use.
   * @param outlinkDb
   *          The outlink database to use.
   * @param output
   *          The output directory.
   * 
   * @throws IOException
   *           If an error occurs while running the inverter job.
   */
  private void runInverter(Path nodeDb, Path outlinkDb, Path output)
      throws IOException, InterruptedException, ClassNotFoundException {

    // configure the inverter
    Job inverter = NutchJob.getInstance(getConf());
    Configuration conf = inverter.getConfiguration();
    inverter.setJobName("LinkAnalysis Inverter");
    FileInputFormat.addInputPath(inverter, nodeDb);
    FileInputFormat.addInputPath(inverter, outlinkDb);
    FileOutputFormat.setOutputPath(inverter, output);
    inverter.setInputFormatClass(SequenceFileInputFormat.class);
    inverter.setJarByClass(Inverter.class);
    inverter.setMapperClass(Inverter.InvertMapper.class);
    inverter.setReducerClass(Inverter.InvertReducer.class);
    inverter.setMapOutputKeyClass(Text.class);
    inverter.setMapOutputValueClass(ObjectWritable.class);
    inverter.setOutputKeyClass(Text.class);
    inverter.setOutputValueClass(LinkDatum.class);
    inverter.setOutputFormatClass(SequenceFileOutputFormat.class);
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
        false);

    // run the inverter job
    LOG.info("Starting inverter job");
    try {
      boolean success = inverter.waitForCompletion(true);
      if (!success) {
        String message = "Inverter job did not succeed, job status:"
            + inverter.getStatus().getState() + ", reason: "
            + inverter.getStatus().getFailureInfo();
        LOG.error(message);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("Inverter job failed:", e);
      throw e;
    }
    LOG.info("Finished inverter job.");
  }

  /**
   * Runs the link analysis job. The link analysis job applies the link rank
   * formula to create a score per url and stores that score in the NodeDb.
   * 
   * Typically the link analysis job is run a number of times to allow the link
   * rank scores to converge.
   * 
   * @param nodeDb
   *          The node database from which we are getting previous link rank
   *          scores.
   * @param inverted
   *          The inverted inlinks
   * @param output
   *          The link analysis output.
   * @param iteration
   *          The current iteration number.
   * @param numIterations
   *          The total number of link analysis iterations
   * 
   * @throws IOException
   *           If an error occurs during link analysis.
   */
  private void runAnalysis(Path nodeDb, Path inverted, Path output,
      int iteration, int numIterations, float rankOne) 
      throws IOException, InterruptedException, ClassNotFoundException {

    Job analyzer = NutchJob.getInstance(getConf());
    Configuration conf = analyzer.getConfiguration();
    conf.set("link.analyze.iteration", String.valueOf(iteration + 1));
    analyzer.setJobName("LinkAnalysis Analyzer, iteration " + (iteration + 1)
        + " of " + numIterations);
    FileInputFormat.addInputPath(analyzer, nodeDb);
    FileInputFormat.addInputPath(analyzer, inverted);
    FileOutputFormat.setOutputPath(analyzer, output);
    conf.set("link.analyze.rank.one", String.valueOf(rankOne));
    analyzer.setMapOutputKeyClass(Text.class);
    analyzer.setMapOutputValueClass(ObjectWritable.class);
    analyzer.setInputFormatClass(SequenceFileInputFormat.class);
    analyzer.setJarByClass(Analyzer.class);
    analyzer.setMapperClass(Analyzer.AnalyzerMapper.class);
    analyzer.setReducerClass(Analyzer.AnalyzerReducer.class);
    analyzer.setOutputKeyClass(Text.class);
    analyzer.setOutputValueClass(Node.class);
    analyzer.setOutputFormatClass(MapFileOutputFormat.class);
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
        false);

    LOG.info("Starting analysis job");
    try {
      boolean success = analyzer.waitForCompletion(true);
      if (!success) {
        String message = "Analysis job did not succeed, job status:"
            + analyzer.getStatus().getState() + ", reason: "
            + analyzer.getStatus().getFailureInfo();
        LOG.error(message);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("Analysis job failed:", e);
      throw e;
    }
    LOG.info("Finished analysis job.");
  }

  /**
   * The Counter job that determines the total number of nodes in the WebGraph.
   * This is used to determine a rank one score for pages with zero inlinks but
   * that contain outlinks.
   */
  private static class Counter {

    private static Text numNodes = new Text(NUM_NODES);
    private static LongWritable one = new LongWritable(1L);

    /**
     * Outputs one for every node.
     */
    public static class CountMapper extends
        Mapper<Text, Node, Text, LongWritable> {
      public void setup(Mapper<Text, Node, Text, LongWritable>.Context context) {
      }

      public void map(Text key, Node value,
          Context context)
          throws IOException, InterruptedException {
        context.write(numNodes, one);
      }
    }

    /**
     * Totals the node number and outputs a single total value.
     */
    public static class CountReducer extends
        Reducer<Text, LongWritable, Text, LongWritable> {
      public void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context) {
      }

      public void reduce(Text key, Iterable<LongWritable> values,
          Context context)
          throws IOException, InterruptedException {

        long total = 0;
        for (LongWritable val : values) {
          total += val.get();
        }
        context.write(numNodes, new LongWritable(total));
      }
    }

  }

  private static class Initializer extends Mapper<Text, Node, Text, Node> {

    private Configuration conf;
    private float initialScore = 1.0f;

    @Override
    public void setup(Mapper<Text, Node, Text, Node>.Context context) {
      conf = context.getConfiguration();
      initialScore = conf.getFloat("link.analyze.initial.score", 1.0f);
    }

    @Override
    public void map(Text key, Node node, Context context) 
        throws IOException, InterruptedException {

      String url = key.toString();
      Node outNode = WritableUtils.clone(node, conf);
      outNode.setInlinkScore(initialScore);

      context.write(new Text(url), outNode);
    }

  }

  /**
   * Inverts outlinks and attaches current score from the NodeDb of the
   * WebGraph. The link analysis process consists of inverting, analyzing and
   * scoring, in a loop for a given number of iterations.
   */
  private static class Inverter {

    /**
     * Convert values to ObjectWritable
     */
    public static class InvertMapper extends 
        Mapper<Text, Writable, Text, ObjectWritable> {

      @Override
      public void setup(Mapper<Text, Writable, Text, ObjectWritable>.Context context) {
      }

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
     * Inverts outlinks to inlinks, attaches current score for the outlink from
     * the NodeDb of the WebGraph.
     */
    public static class InvertReducer extends
        Reducer<Text, ObjectWritable, Text, LinkDatum> {

      private Configuration conf;      

      @Override
      public void setup(Reducer<Text, ObjectWritable, Text, LinkDatum>.Context context) {
        conf = context.getConfiguration();
      }

      @Override
      public void reduce(Text key, Iterable<ObjectWritable> values,
          Context context)
          throws IOException, InterruptedException {

        String fromUrl = key.toString();
        List<LinkDatum> outlinks = new ArrayList<>();
        Node node = null;

        // aggregate outlinks, assign other values
        for (ObjectWritable write : values) {
          Object obj = write.get();
          if (obj instanceof Node) {
            node = (Node) obj;
          } else if (obj instanceof LinkDatum) {
            outlinks.add(WritableUtils.clone((LinkDatum) obj, conf));
          }
        }

        // get the number of outlinks and the current inlink and outlink scores
        // from the node of the url
        int numOutlinks = node.getNumOutlinks();
        float inlinkScore = node.getInlinkScore();
        float outlinkScore = node.getOutlinkScore();
        LOG.debug(fromUrl + ": num outlinks " + numOutlinks);

        // can't invert if no outlinks
        if (numOutlinks > 0) {
          for (int i = 0; i < outlinks.size(); i++) {
            LinkDatum outlink = outlinks.get(i);
            String toUrl = outlink.getUrl();

            outlink.setUrl(fromUrl);
            outlink.setScore(outlinkScore);

            // collect the inverted outlink
            context.write(new Text(toUrl), outlink);
            LOG.debug(toUrl + ": inverting inlink from " + fromUrl
                + " origscore: " + inlinkScore + " numOutlinks: " + numOutlinks
                + " inlinkscore: " + outlinkScore);
          }
        }
      }
    }
  }

  /**
   * Runs a single link analysis iteration.
   */
  private static class Analyzer {

    /**
     * Convert values to ObjectWritable
     */
    public static class AnalyzerMapper extends 
        Mapper<Text, Writable, Text, ObjectWritable> {

      private Configuration conf;

      /**
       * Configures the job mapper, sets the damping factor, rank one score, and other
       * needed values for analysis.
       */
      @Override
      public void setup(Mapper<Text, Writable, Text, ObjectWritable>.Context context) {
        conf = context.getConfiguration();
      }

      @Override
      public void map(Text key, Writable value,
          Context context)
          throws IOException, InterruptedException {

        ObjectWritable objWrite = new ObjectWritable();
        objWrite.set(WritableUtils.clone(value, conf));
        context.write(key, objWrite);
      }
    }

    /**
     * Performs a single iteration of link analysis. The resulting scores are
     * stored in a temporary NodeDb which replaces the NodeDb of the WebGraph.
     */
    public static class AnalyzerReducer extends
        Reducer<Text, ObjectWritable, Text, Node> {

      private Configuration conf;
      private float dampingFactor = 0.85f;
      private float rankOne = 0.0f;
      private int itNum = 0;
      private boolean limitPages = true;
      private boolean limitDomains = true;

      /**
       * Configures the job reducer, sets the damping factor, rank one score, and other
       * needed values for analysis.
       */
      @Override
      public void setup(
          Reducer<Text, ObjectWritable, Text, Node>.Context context) {
        conf = context.getConfiguration();
        dampingFactor = conf.getFloat("link.analyze.damping.factor", 0.85f);
        rankOne = conf.getFloat("link.analyze.rank.one", 0.0f);
        itNum = conf.getInt("link.analyze.iteration", 0);
        limitPages = conf.getBoolean("link.ignore.limit.page", true);
        limitDomains = conf.getBoolean("link.ignore.limit.domain", true);
      }

      @Override
      public void reduce(Text key, Iterable<ObjectWritable> values,
          Context context)
          throws IOException, InterruptedException {

        String url = key.toString();
        Set<String> domains = new HashSet<>();
        Set<String> pages = new HashSet<>();
        Node node = null;

        // a page with zero inlinks has a score of rankOne
        int numInlinks = 0;
        float totalInlinkScore = rankOne;

        for (ObjectWritable next : values) {

          Object value = next.get();
          if (value instanceof Node) {
            node = (Node) value;
          } else if (value instanceof LinkDatum) {

            LinkDatum linkDatum = (LinkDatum) value;
            float scoreFromInlink = linkDatum.getScore();
            String inlinkUrl = linkDatum.getUrl();
            String inLinkDomain = URLUtil.getDomainName(inlinkUrl);
            String inLinkPage = URLUtil.getPage(inlinkUrl);

            // limit counting duplicate inlinks by pages or domains
            if ((limitPages && pages.contains(inLinkPage))
                || (limitDomains && domains.contains(inLinkDomain))) {
              LOG.debug(url + ": ignoring " + scoreFromInlink + " from "
                  + inlinkUrl + ", duplicate page or domain");
              continue;
            }

            // aggregate total inlink score
            numInlinks++;
            totalInlinkScore += scoreFromInlink;
            domains.add(inLinkDomain);
            pages.add(inLinkPage);
            LOG.debug(url + ": adding " + scoreFromInlink + " from " + inlinkUrl
                + ", total: " + totalInlinkScore);
          }
        }

        // calculate linkRank score formula
        float linkRankScore = (1 - dampingFactor)
            + (dampingFactor * totalInlinkScore);

        LOG.debug(url + ": score: " + linkRankScore + " num inlinks: "
            + numInlinks + " iteration: " + itNum);

        // store the score in a temporary NodeDb
        Node outNode = WritableUtils.clone(node, conf);
        outNode.setInlinkScore(linkRankScore);
        context.write(key, outNode);
      }
    }
  }

  /**
   * Default constructor.
   */
  public LinkRank() {
    super();
  }

  /**
   * Configurable constructor.
   */
  public LinkRank(Configuration conf) {
    super(conf);
  }

  public void close() {
  }

  /**
   * Runs the complete link analysis job. The complete job determins rank one
   * score. Then runs through a given number of invert and analyze iterations,
   * by default 10. And finally replaces the NodeDb in the WebGraph with the
   * link rank output.
   * 
   * @param webGraphDb
   *          The WebGraph to run link analysis on.
   * 
   * @throws IOException
   *           If an error occurs during link analysis.
   */
  public void analyze(Path webGraphDb) throws IOException, 
      ClassNotFoundException, InterruptedException {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("Analysis: starting at " + sdf.format(start));

    // store the link rank under the webgraphdb temporarily, final scores get
    // upddated into the nodedb
    Path linkRank = new Path(webGraphDb, "linkrank");
    Configuration conf = getConf();
    FileSystem fs = linkRank.getFileSystem(conf);

    // create the linkrank directory if needed
    if (!fs.exists(linkRank)) {
      fs.mkdirs(linkRank);
    }

    // the webgraph outlink and node database paths
    Path wgOutlinkDb = new Path(webGraphDb, WebGraph.OUTLINK_DIR);
    Path wgNodeDb = new Path(webGraphDb, WebGraph.NODE_DIR);
    Path nodeDb = new Path(linkRank, WebGraph.NODE_DIR);

    // get the number of total nodes in the webgraph, used for rank one, then
    // initialze all urls with a default score
    int numLinks = runCounter(fs, webGraphDb);
    runInitializer(wgNodeDb, nodeDb);
    float rankOneScore = (1f / (float) numLinks);

    if (LOG.isInfoEnabled()) {
      LOG.info("Analysis: Number of links: " + numLinks);
      LOG.info("Analysis: Rank One: " + rankOneScore);
    }

    // run invert and analysis for a given number of iterations to allow the
    // link rank scores to converge
    int numIterations = conf.getInt("link.analyze.num.iterations", 10);
    for (int i = 0; i < numIterations; i++) {

      // the input to inverting is always the previous output from analysis
      LOG.info("Analysis: Starting iteration " + (i + 1) + " of "
          + numIterations);
      Path tempRank = new Path(linkRank + "-"
          + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
      fs.mkdirs(tempRank);
      Path tempInverted = new Path(tempRank, "inverted");
      Path tempNodeDb = new Path(tempRank, WebGraph.NODE_DIR);

      // run invert and analysis
      runInverter(nodeDb, wgOutlinkDb, tempInverted);
      runAnalysis(nodeDb, tempInverted, tempNodeDb, i, numIterations,
          rankOneScore);

      // replace the temporary NodeDb with the output from analysis
      LOG.info("Analysis: Installing new link scores");
      FSUtils.replace(fs, linkRank, tempRank, true);
      LOG.info("Analysis: finished iteration " + (i + 1) + " of "
          + numIterations);
    }

    // replace the NodeDb in the WebGraph with the final output of analysis
    LOG.info("Analysis: Installing web graph nodes");
    FSUtils.replace(fs, wgNodeDb, nodeDb, true);

    // remove the temporary link rank folder
    fs.delete(linkRank, true);
    long end = System.currentTimeMillis();
    LOG.info("Analysis: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new LinkRank(), args);
    System.exit(res);
  }

  /**
   * Runs the LinkRank tool.
   */
  public int run(String[] args) throws Exception {

    Options options = new Options();
    OptionBuilder.withArgName("help");
    OptionBuilder.withDescription("show this help message");
    Option helpOpts = OptionBuilder.create("help");
    options.addOption(helpOpts);

    OptionBuilder.withArgName("webgraphdb");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("the web graph db to use");
    Option webgraphOpts = OptionBuilder.create("webgraphdb");
    options.addOption(webgraphOpts);

    CommandLineParser parser = new GnuParser();
    try {

      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("webgraphdb")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("LinkRank", options);
        return -1;
      }

      String webGraphDb = line.getOptionValue("webgraphdb");

      analyze(new Path(webGraphDb));
      return 0;
    } catch (Exception e) {
      LOG.error("LinkAnalysis: " + StringUtils.stringifyException(e));
      return -2;
    }
  }
}
