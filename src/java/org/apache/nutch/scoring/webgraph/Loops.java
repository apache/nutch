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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.FSUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

/**
 * The Loops job identifies cycles of loops inside of the web graph. This is
 * then used in the LinkRank program to remove those links from consideration
 * during link analysis.
 * 
 * This job will identify both reciprocal links and cycles of 2+ links up to a
 * set depth to check. The Loops job is expensive in both computational and
 * space terms. Because it checks outlinks of outlinks of outlinks for cycles
 * its intermediate output can be extremly large even if the end output is
 * rather small. Because of this the Loops job is optional and if it doesn't
 * exist then it won't be factored into the LinkRank program.
 */
public class Loops
  extends Configured
  implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(Loops.class);
  public static final String LOOPS_DIR = "loops";
  public static final String ROUTES_DIR = "routes";

  /**
   * A link path or route looking to identify a link cycle.
   */
  public static class Route
    implements Writable {

    private String outlinkUrl = null;
    private String lookingFor = null;
    private boolean found = false;

    public Route() {

    }

    public String getOutlinkUrl() {
      return outlinkUrl;
    }

    public void setOutlinkUrl(String outlinkUrl) {
      this.outlinkUrl = outlinkUrl;
    }

    public String getLookingFor() {
      return lookingFor;
    }

    public void setLookingFor(String lookingFor) {
      this.lookingFor = lookingFor;
    }

    public boolean isFound() {
      return found;
    }

    public void setFound(boolean found) {
      this.found = found;
    }

    public void readFields(DataInput in)
      throws IOException {

      outlinkUrl = Text.readString(in);
      lookingFor = Text.readString(in);
      found = in.readBoolean();
    }

    public void write(DataOutput out)
      throws IOException {
      Text.writeString(out, outlinkUrl);
      Text.writeString(out, lookingFor);
      out.writeBoolean(found);
    }
  }

  /**
   * A set of loops.
   */
  public static class LoopSet
    implements Writable {

    private Set<String> loopSet = new HashSet<String>();

    public LoopSet() {

    }

    public Set<String> getLoopSet() {
      return loopSet;
    }

    public void setLoopSet(Set<String> loopSet) {
      this.loopSet = loopSet;
    }

    public void readFields(DataInput in)
      throws IOException {

      int numNodes = in.readInt();
      loopSet = new HashSet<String>();
      for (int i = 0; i < numNodes; i++) {
        String url = Text.readString(in);
        loopSet.add(url);
      }
    }

    public void write(DataOutput out)
      throws IOException {

      int numNodes = (loopSet != null ? loopSet.size() : 0);
      out.writeInt(numNodes);
      for (String loop : loopSet) {
        Text.writeString(out, loop);
      }
    }

    public String toString() {
      StringBuilder builder = new StringBuilder();
      for (String loop : loopSet) {
        builder.append(loop + ",");
      }
      return builder.substring(0, builder.length() - 1);
    }
  }

  /**
   * Initializes the Loop routes.
   */
  public static class Initializer
    extends Configured
    implements Mapper<Text, Writable, Text, ObjectWritable>,
    Reducer<Text, ObjectWritable, Text, Route> {

    private JobConf conf;

    /**
     * Default constructor.
     */
    public Initializer() {
    }

    /**
     * Configurable constructor.
     */
    public Initializer(Configuration conf) {
      setConf(conf);
    }

    /**
     * Configure the job.
     */
    public void configure(JobConf conf) {
      this.conf = conf;
    }

    /**
     * Wraps values in ObjectWritable.
     */
    public void map(Text key, Writable value,
      OutputCollector<Text, ObjectWritable> output, Reporter reporter)
      throws IOException {

      ObjectWritable objWrite = new ObjectWritable();
      objWrite.set(value);
      output.collect(key, objWrite);
    }

    /**
     * Takes any node that has inlinks and sets up a route for all of its
     * outlinks. These routes will then be followed to a maximum depth inside of
     * the Looper job.
     */
    public void reduce(Text key, Iterator<ObjectWritable> values,
      OutputCollector<Text, Route> output, Reporter reporter)
      throws IOException {

      String url = key.toString();
      Node node = null;
      List<LinkDatum> outlinkList = new ArrayList<LinkDatum>();

      // collect all outlinks and assign node
      while (values.hasNext()) {
        ObjectWritable objWrite = values.next();
        Object obj = objWrite.get();
        if (obj instanceof LinkDatum) {
          outlinkList.add((LinkDatum)obj);
        }
        else if (obj instanceof Node) {
          node = (Node)obj;
        }
      }

      // has to have inlinks otherwise cycle not possible
      if (node != null) {

        int numInlinks = node.getNumInlinks();
        if (numInlinks > 0) {

          // initialize and collect a route for every outlink
          for (LinkDatum datum : outlinkList) {
            String outlinkUrl = datum.getUrl();
            Route route = new Route();
            route.setFound(false);
            route.setLookingFor(url);
            route.setOutlinkUrl(outlinkUrl);
            output.collect(new Text(outlinkUrl), route);
          }
        }
      }
    }

    public void close() {
    }
  }

  /**
   * Follows a route path looking for the start url of the route. If the start
   * url is found then the route is a cyclical path.
   */
  public static class Looper
    extends Configured
    implements Mapper<Text, Writable, Text, ObjectWritable>,
    Reducer<Text, ObjectWritable, Text, Route> {

    private JobConf conf;
    private boolean last = false;

    /**
     * Default constructor.
     */
    public Looper() {
    }

    /**
     * Configurable constructor.
     */
    public Looper(Configuration conf) {
      setConf(conf);
    }

    /**
     * Configure the job.
     */
    public void configure(JobConf conf) {
      this.conf = conf;
      this.last = conf.getBoolean("last", false);
    }

    /**
     * Wrap values in ObjectWritable.
     */
    public void map(Text key, Writable value,
      OutputCollector<Text, ObjectWritable> output, Reporter reporter)
      throws IOException {

      ObjectWritable objWrite = new ObjectWritable();
      Writable cloned = null;
      if (value instanceof LinkDatum) {
        cloned = new Text(((LinkDatum)value).getUrl());
      }
      else {
        cloned = WritableUtils.clone(value, conf);
      }
      objWrite.set(cloned);
      output.collect(key, objWrite);
    }

    /**
     * Performs a single loop pass looking for loop cycles within routes. If
     * This is not the last loop cycle then url will be mapped for further
     * passes.
     */
    public void reduce(Text key, Iterator<ObjectWritable> values,
      OutputCollector<Text, Route> output, Reporter reporter)
      throws IOException {

      List<Route> routeList = new ArrayList<Route>();
      Set<String> outlinkUrls = new LinkedHashSet<String>();
      int numValues = 0;

      // aggregate all routes and outlinks for a given url
      while (values.hasNext()) {
        ObjectWritable next = values.next();
        Object value = next.get();
        if (value instanceof Route) {
          routeList.add((Route)WritableUtils.clone((Route)value, conf));
        }
        else if (value instanceof Text) {
          String outlinkUrl = ((Text)value).toString();
          if (!outlinkUrls.contains(outlinkUrl)) {
            outlinkUrls.add(outlinkUrl);
          }
        }

        // specify progress, could be a lot of routes
        numValues++;
        if (numValues % 100 == 0) {
          reporter.progress();
        }
      }

      // loop through the route list
      Iterator<Route> routeIt = routeList.listIterator();
      while (routeIt.hasNext()) {

        // removing the route for space concerns, could be a lot of routes
        // if the route is already found, meaning it is a loop just collect it
        // urls with no outlinks that are not found will fall off
        Route route = routeIt.next();
        routeIt.remove();
        if (route.isFound()) {
          output.collect(key, route);
        }
        else {

          // if the route start url is found, set route to found and collect
          String lookingFor = route.getLookingFor();
          if (outlinkUrls.contains(lookingFor)) {
            route.setFound(true);
            output.collect(key, route);
          }
          else if (!last) {

            // setup for next pass through the loop
            for (String outlink : outlinkUrls) {
              output.collect(new Text(outlink), route);
            }
          }
        }
      }
    }

    public void close() {
    }
  }

  /**
   * Finishes the Loops job by aggregating and collecting and found routes.
   */
  public static class Finalizer
    extends Configured
    implements Mapper<Text, Route, Text, Route>,
    Reducer<Text, Route, Text, LoopSet> {

    private JobConf conf;

    /**
     * Default constructor.
     */
    public Finalizer() {
    }

    /**
     * Configurable constructor.
     */
    public Finalizer(Configuration conf) {
      setConf(conf);
    }

    /**
     * Configures the job.
     */
    public void configure(JobConf conf) {
      this.conf = conf;
    }

    /**
     * Maps out and found routes, those will be the link cycles.
     */
    public void map(Text key, Route value, OutputCollector<Text, Route> output,
      Reporter reporter)
      throws IOException {

      if (value.isFound()) {
        String lookingFor = value.getLookingFor();
        output.collect(new Text(lookingFor), value);
      }
    }

    /**
     * Aggregates all found routes for a given start url into a loopset and 
     * collects the loopset.
     */
    public void reduce(Text key, Iterator<Route> values,
      OutputCollector<Text, LoopSet> output, Reporter reporter)
      throws IOException {

      LoopSet loops = new LoopSet();
      while (values.hasNext()) {
        Route route = values.next();
        loops.getLoopSet().add(route.getOutlinkUrl());
      }
      output.collect(key, loops);
    }

    public void close() {
    }
  }

  /**
   * Runs the various loop jobs.
   */
  public void findLoops(Path webGraphDb)
    throws IOException {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("Loops: starting at " + sdf.format(start));
      LOG.info("Loops: webgraphdb: " + webGraphDb);
    }

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    Path outlinkDb = new Path(webGraphDb, WebGraph.OUTLINK_DIR);
    Path nodeDb = new Path(webGraphDb, WebGraph.NODE_DIR);
    Path routes = new Path(webGraphDb, ROUTES_DIR);
    Path tempRoute = new Path(webGraphDb, ROUTES_DIR + "-"
      + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    // run the initializer
    JobConf init = new NutchJob(conf);
    init.setJobName("Initializer: " + webGraphDb);
    FileInputFormat.addInputPath(init, outlinkDb);
    FileInputFormat.addInputPath(init, nodeDb);
    init.setInputFormat(SequenceFileInputFormat.class);
    init.setMapperClass(Initializer.class);
    init.setReducerClass(Initializer.class);
    init.setMapOutputKeyClass(Text.class);
    init.setMapOutputValueClass(ObjectWritable.class);
    init.setOutputKeyClass(Text.class);
    init.setOutputValueClass(Route.class);
    FileOutputFormat.setOutputPath(init, tempRoute);
    init.setOutputFormat(SequenceFileOutputFormat.class);

    try {
      LOG.info("Loops: starting initializer");
      JobClient.runJob(init);
      LOG.info("Loops: installing initializer " + routes);
      FSUtils.replace(fs, routes, tempRoute, true);
      LOG.info("Loops: finished initializer");
    }
    catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }

    // run the loops job for a maxdepth, default 2, which will find a 3 link
    // loop cycle
    int depth = conf.getInt("link.loops.depth", 2);
    for (int i = 0; i < depth; i++) {

      JobConf looper = new NutchJob(conf);
      looper.setJobName("Looper: " + (i + 1) + " of " + depth);
      FileInputFormat.addInputPath(looper, outlinkDb);
      FileInputFormat.addInputPath(looper, routes);
      looper.setInputFormat(SequenceFileInputFormat.class);
      looper.setMapperClass(Looper.class);
      looper.setReducerClass(Looper.class);
      looper.setMapOutputKeyClass(Text.class);
      looper.setMapOutputValueClass(ObjectWritable.class);
      looper.setOutputKeyClass(Text.class);
      looper.setOutputValueClass(Route.class);
      FileOutputFormat.setOutputPath(looper, tempRoute);
      looper.setOutputFormat(SequenceFileOutputFormat.class);
      looper.setBoolean("last", i == (depth - 1));

      try {
        LOG.info("Loops: starting looper");
        JobClient.runJob(looper);
        LOG.info("Loops: installing looper " + routes);
        FSUtils.replace(fs, routes, tempRoute, true);
        LOG.info("Loops: finished looper");
      }
      catch (IOException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw e;
      }
    }

    // run the finalizer
    JobConf finalizer = new NutchJob(conf);
    finalizer.setJobName("Finalizer: " + webGraphDb);
    FileInputFormat.addInputPath(finalizer, routes);
    finalizer.setInputFormat(SequenceFileInputFormat.class);
    finalizer.setMapperClass(Finalizer.class);
    finalizer.setReducerClass(Finalizer.class);
    finalizer.setMapOutputKeyClass(Text.class);
    finalizer.setMapOutputValueClass(Route.class);
    finalizer.setOutputKeyClass(Text.class);
    finalizer.setOutputValueClass(LoopSet.class);
    FileOutputFormat.setOutputPath(finalizer, new Path(webGraphDb, LOOPS_DIR));
    finalizer.setOutputFormat(MapFileOutputFormat.class);

    try {
      LOG.info("Loops: starting finalizer");
      JobClient.runJob(finalizer);
      LOG.info("Loops: finished finalizer");
    }
    catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }
    long end = System.currentTimeMillis();
    LOG.info("Loops: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args)
    throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new Loops(), args);
    System.exit(res);
  }

  /**
   * Runs the Loops tool.
   */
  public int run(String[] args)
    throws Exception {

    Options options = new Options();
    Option helpOpts = OptionBuilder.withArgName("help").withDescription(
      "show this help message").create("help");
    Option webGraphDbOpts = OptionBuilder.withArgName("webgraphdb").hasArg().withDescription(
      "the web graph database to use").create("webgraphdb");
    options.addOption(helpOpts);
    options.addOption(webGraphDbOpts);

    CommandLineParser parser = new GnuParser();
    try {

      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("webgraphdb")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Loops", options);
        return -1;
      }

      String webGraphDb = line.getOptionValue("webgraphdb");
      findLoops(new Path(webGraphDb));
      return 0;
    }
    catch (Exception e) {
      LOG.error("Loops: " + StringUtils.stringifyException(e));
      return -2;
    }
  }
}
