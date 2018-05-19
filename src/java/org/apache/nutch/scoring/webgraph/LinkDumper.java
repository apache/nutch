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
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.FSUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

/**
 * The LinkDumper tool creates a database of node to inlink information that can
 * be read using the nested Reader class. This allows the inlink and scoring
 * state of a single url to be reviewed quickly to determine why a given url is
 * ranking a certain way. This tool is to be used with the LinkRank analysis.
 */
public class LinkDumper extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  public static final String DUMP_DIR = "linkdump";

  /**
   * Reader class which will print out the url and all of its inlinks to system
   * out. Each inlinkwill be displayed with its node information including score
   * and number of in and outlinks.
   */
  public static class Reader {

    public static void main(String[] args) throws Exception {

      if (args == null || args.length < 2) {
        System.out.println("LinkDumper$Reader usage: <webgraphdb> <url>");
        return;
      }

      // open the readers for the linkdump directory
      Configuration conf = NutchConfiguration.create();
      Path webGraphDb = new Path(args[0]);
      String url = args[1];
      MapFile.Reader[] readers = MapFileOutputFormat.getReaders(new Path(
          webGraphDb, DUMP_DIR), conf);

      // get the link nodes for the url
      Text key = new Text(url);
      LinkNodes nodes = new LinkNodes();
      MapFileOutputFormat.getEntry(readers,
          new HashPartitioner<>(), key, nodes);

      // print out the link nodes
      LinkNode[] linkNodesAr = nodes.getLinks();
      System.out.println(url + ":");
      for (LinkNode node : linkNodesAr) {
        System.out.println("  " + node.getUrl() + " - "
            + node.getNode().toString());
      }

      // close the readers
      FSUtils.closeReaders(readers);
    }
  }

  /**
   * Bean class which holds url to node information.
   */
  public static class LinkNode implements Writable {

    private String url = null;
    private Node node = null;

    public LinkNode() {

    }

    public LinkNode(String url, Node node) {
      this.url = url;
      this.node = node;
    }

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public Node getNode() {
      return node;
    }

    public void setNode(Node node) {
      this.node = node;
    }

    public void readFields(DataInput in) throws IOException {
      url = in.readUTF();
      node = new Node();
      node.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
      out.writeUTF(url);
      node.write(out);
    }

  }

  /**
   * Writable class which holds an array of LinkNode objects.
   */
  public static class LinkNodes implements Writable {

    private LinkNode[] links;

    public LinkNodes() {

    }

    public LinkNodes(LinkNode[] links) {
      this.links = links;
    }

    public LinkNode[] getLinks() {
      return links;
    }

    public void setLinks(LinkNode[] links) {
      this.links = links;
    }

    public void readFields(DataInput in) throws IOException {
      int numLinks = in.readInt();
      if (numLinks > 0) {
        links = new LinkNode[numLinks];
        for (int i = 0; i < numLinks; i++) {
          LinkNode node = new LinkNode();
          node.readFields(in);
          links[i] = node;
        }
      }
    }

    public void write(DataOutput out) throws IOException {
      if (links != null && links.length > 0) {
        int numLinks = links.length;
        out.writeInt(numLinks);
        for (int i = 0; i < numLinks; i++) {
          links[i].write(out);
        }
      }
    }
  }

  /**
   * Inverts outlinks from the WebGraph to inlinks and attaches node
   * information.
   */
  public static class Inverter {

    /**
     * Wraps all values in ObjectWritables.
     */
    public static class InvertMapper extends 
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
     * Inverts outlinks to inlinks while attaching node information to the
     * outlink.
     */
    public static class InvertReducer extends
        Reducer<Text, ObjectWritable, Text, LinkNode> {

      private Configuration conf;

      @Override
      public void setup(Reducer<Text, ObjectWritable, Text, LinkNode>.Context context) {
        conf = context.getConfiguration();
      }

      @Override
      public void reduce(Text key, Iterable<ObjectWritable> values,
          Context context)
          throws IOException, InterruptedException {

        String fromUrl = key.toString();
        List<LinkDatum> outlinks = new ArrayList<>();
        Node node = null;
        
        // loop through all values aggregating outlinks, saving node
        for (ObjectWritable write : values) {
          Object obj = write.get();
          if (obj instanceof Node) {
            node = (Node) obj;
          } else if (obj instanceof LinkDatum) {
            outlinks.add(WritableUtils.clone((LinkDatum) obj, conf));
          }
        }

        // only collect if there are outlinks
        int numOutlinks = node.getNumOutlinks();
        if (numOutlinks > 0) {
          for (int i = 0; i < outlinks.size(); i++) {
            LinkDatum outlink = outlinks.get(i);
            String toUrl = outlink.getUrl();

            // collect the outlink as an inlink with the node
            context.write(new Text(toUrl), new LinkNode(fromUrl, node));
          }
        }
      }
    }

    public void cleanup() {
    }
  }

  /**
   * Merges LinkNode objects into a single array value per url. This allows all
   * values to be quickly retrieved and printed via the Reader tool.
   */
  public static class Merger extends
      Reducer<Text, LinkNode, Text, LinkNodes> {

    private Configuration conf;
    private int maxInlinks = 50000;

    /**
     * Aggregate all LinkNode objects for a given url.
     */
    @Override
    public void reduce(Text key, Iterable<LinkNode> values,
        Context context)
        throws IOException, InterruptedException {

      List<LinkNode> nodeList = new ArrayList<>();
      int numNodes = 0;

      for (LinkNode cur : values) {
        if (numNodes < maxInlinks) {
          nodeList.add(WritableUtils.clone(cur, conf));
          numNodes++;
        } else {
          break;
        }
      }

      LinkNode[] linkNodesAr = nodeList.toArray(new LinkNode[nodeList.size()]);
      LinkNodes linkNodes = new LinkNodes(linkNodesAr);
      context.write(key, linkNodes);
    }
  }

  /**
   * Runs the inverter and merger jobs of the LinkDumper tool to create the url
   * to inlink node database.
   */
  public void dumpLinks(Path webGraphDb) throws IOException, 
      InterruptedException, ClassNotFoundException {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("NodeDumper: starting at " + sdf.format(start));
    Configuration conf = getConf();
    FileSystem fs = webGraphDb.getFileSystem(conf);

    Path linkdump = new Path(webGraphDb, DUMP_DIR);
    Path nodeDb = new Path(webGraphDb, WebGraph.NODE_DIR);
    Path outlinkDb = new Path(webGraphDb, WebGraph.OUTLINK_DIR);

    // run the inverter job
    Path tempInverted = new Path(webGraphDb, "inverted-"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
    Job inverter = NutchJob.getInstance(conf);
    inverter.setJobName("LinkDumper: inverter");
    FileInputFormat.addInputPath(inverter, nodeDb);
    FileInputFormat.addInputPath(inverter, outlinkDb);
    inverter.setInputFormatClass(SequenceFileInputFormat.class);
    inverter.setJarByClass(Inverter.class);
    inverter.setMapperClass(Inverter.InvertMapper.class);
    inverter.setReducerClass(Inverter.InvertReducer.class);
    inverter.setMapOutputKeyClass(Text.class);
    inverter.setMapOutputValueClass(ObjectWritable.class);
    inverter.setOutputKeyClass(Text.class);
    inverter.setOutputValueClass(LinkNode.class);
    FileOutputFormat.setOutputPath(inverter, tempInverted);
    inverter.setOutputFormatClass(SequenceFileOutputFormat.class);

    try {
      LOG.info("LinkDumper: running inverter");
      boolean success = inverter.waitForCompletion(true);
      if (!success) {
        String message = "LinkDumper inverter job did not succeed, job status:"
            + inverter.getStatus().getState() + ", reason: "
            + inverter.getStatus().getFailureInfo();
        LOG.error(message);
        throw new RuntimeException(message);
      }
      LOG.info("LinkDumper: finished inverter");
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("LinkDumper inverter job failed:", e);
      throw e;
    }

    // run the merger job
    Job merger = NutchJob.getInstance(conf);
    merger.setJobName("LinkDumper: merger");
    FileInputFormat.addInputPath(merger, tempInverted);
    merger.setJarByClass(Merger.class);
    merger.setInputFormatClass(SequenceFileInputFormat.class);
    merger.setReducerClass(Merger.class);
    merger.setMapOutputKeyClass(Text.class);
    merger.setMapOutputValueClass(LinkNode.class);
    merger.setOutputKeyClass(Text.class);
    merger.setOutputValueClass(LinkNodes.class);
    FileOutputFormat.setOutputPath(merger, linkdump);
    merger.setOutputFormatClass(MapFileOutputFormat.class);

    try {
      LOG.info("LinkDumper: running merger");
      boolean success = merger.waitForCompletion(true);
      if (!success) {
        String message = "LinkDumper merger job did not succeed, job status:"
            + merger.getStatus().getState() + ", reason: "
            + merger.getStatus().getFailureInfo();
        LOG.error(message);
        throw new RuntimeException(message);
      }
      LOG.info("LinkDumper: finished merger");
    } catch (IOException e) {
      LOG.error("LinkDumper merger job failed:", e);
      throw e;
    }

    fs.delete(tempInverted, true);
    long end = System.currentTimeMillis();
    LOG.info("LinkDumper: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new LinkDumper(),
        args);
    System.exit(res);
  }

  /**
   * Runs the LinkDumper tool. This simply creates the database, to read the
   * values the nested Reader tool must be used.
   */
  public int run(String[] args) throws Exception {

    Options options = new Options();
    OptionBuilder.withArgName("help");
    OptionBuilder.withDescription("show this help message");
    Option helpOpts = OptionBuilder.create("help");
    options.addOption(helpOpts);

    OptionBuilder.withArgName("webgraphdb");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("the web graph database to use");
    Option webGraphDbOpts = OptionBuilder.create("webgraphdb");
    options.addOption(webGraphDbOpts);

    CommandLineParser parser = new GnuParser();
    try {

      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("webgraphdb")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("LinkDumper", options);
        return -1;
      }

      String webGraphDb = line.getOptionValue("webgraphdb");
      dumpLinks(new Path(webGraphDb));
      return 0;
    } catch (Exception e) {
      LOG.error("LinkDumper: " + StringUtils.stringifyException(e));
      return -2;
    }
  }
}
