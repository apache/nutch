package org.apache.nutch.scoring.webgraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
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
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.scoring.webgraph.Loops.LoopSet;
import org.apache.nutch.util.FSUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/**
 * The LinkDumper tool creates a database of node to inlink information that can
 * be read using the nested Reader class.  This allows the inlink and scoring 
 * state of a single url to be reviewed quickly to determine why a given url is 
 * ranking a certain way.  This tool is to be used with the LinkRank analysis.
 */
public class LinkDumper
  extends Configured
  implements Tool {

  public static final Log LOG = LogFactory.getLog(LinkDumper.class);
  public static final String DUMP_DIR = "linkdump";

  /**
   * Reader class which will print out the url and all of its inlinks to system 
   * out.  Each inlinkwill be displayed with its node information including 
   * score and number of in and outlinks.
   */
  public static class Reader {

    public static void main(String[] args)
      throws Exception {
      
      if (args == null || args.length < 2) {
        System.out.println("LinkDumper$Reader usage: <webgraphdb> <url>");
        return;
      }

      // open the readers for the linkdump directory
      Configuration conf = NutchConfiguration.create();
      FileSystem fs = FileSystem.get(conf);
      Path webGraphDb = new Path(args[0]);
      String url = args[1];
      MapFile.Reader[] readers = MapFileOutputFormat.getReaders(fs, new Path(
        webGraphDb, DUMP_DIR), conf);

      // get the link nodes for the url
      Text key = new Text(url);
      LinkNodes nodes = new LinkNodes();
      MapFileOutputFormat.getEntry(readers,
        new HashPartitioner<Text, LinkNodes>(), key, nodes);

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
  public static class LinkNode
    implements Writable {

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

    public void readFields(DataInput in)
      throws IOException {
      url = in.readUTF();
      node = new Node();
      node.readFields(in);
    }

    public void write(DataOutput out)
      throws IOException {
      out.writeUTF(url);
      node.write(out);
    }

  }

  /**
   * Writable class which holds an array of LinkNode objects.
   */
  public static class LinkNodes
    implements Writable {

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

    public void readFields(DataInput in)
      throws IOException {
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

    public void write(DataOutput out)
      throws IOException {
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
  public static class Inverter
    implements Mapper<Text, Writable, Text, ObjectWritable>,
    Reducer<Text, ObjectWritable, Text, LinkNode> {

    private JobConf conf;

    public void configure(JobConf conf) {
      this.conf = conf;
    }

    /**
     * Wraps all values in ObjectWritables.
     */
    public void map(Text key, Writable value,
      OutputCollector<Text, ObjectWritable> output, Reporter reporter)
      throws IOException {

      ObjectWritable objWrite = new ObjectWritable();
      objWrite.set(value);
      output.collect(key, objWrite);
    }

    /**
     * Inverts outlinks to inlinks while attaching node information to the 
     * outlink.
     */
    public void reduce(Text key, Iterator<ObjectWritable> values,
      OutputCollector<Text, LinkNode> output, Reporter reporter)
      throws IOException {

      String fromUrl = key.toString();
      List<LinkDatum> outlinks = new ArrayList<LinkDatum>();
      Node node = null;
      LoopSet loops = null;

      // loop through all values aggregating outlinks, saving node and loopset
      while (values.hasNext()) {
        ObjectWritable write = values.next();
        Object obj = write.get();
        if (obj instanceof Node) {
          node = (Node)obj;
        }
        else if (obj instanceof LinkDatum) {
          outlinks.add((LinkDatum)WritableUtils.clone((LinkDatum)obj, conf));
        }
        else if (obj instanceof LoopSet) {
          loops = (LoopSet)obj;
        }
      }

      // only collect if there are outlinks
      int numOutlinks = node.getNumOutlinks();
      if (numOutlinks > 0) {

        Set<String> loopSet = (loops != null) ? loops.getLoopSet() : null;
        for (int i = 0; i < outlinks.size(); i++) {
          LinkDatum outlink = outlinks.get(i);
          String toUrl = outlink.getUrl();
          
          // remove any url that is in the loopset, same as LinkRank
          if (loopSet != null && loopSet.contains(toUrl)) {
            continue;
          }
          
          // collect the outlink as an inlink with the node 
          output.collect(new Text(toUrl), new LinkNode(fromUrl, node));
        }
      }
    }

    public void close() {
    }
  }

  /**
   * Merges LinkNode objects into a single array value per url.  This allows 
   * all values to be quickly retrieved and printed via the Reader tool.
   */
  public static class Merger
    implements Reducer<Text, LinkNode, Text, LinkNodes> {

    private JobConf conf;
    private int maxInlinks = 50000;

    public void configure(JobConf conf) {
      this.conf = conf;
    }

    /**
     * Aggregate all LinkNode objects for a given url.
     */
    public void reduce(Text key, Iterator<LinkNode> values,
      OutputCollector<Text, LinkNodes> output, Reporter reporter)
      throws IOException {

      List<LinkNode> nodeList = new ArrayList<LinkNode>();
      int numNodes = 0;

      while (values.hasNext()) {
        LinkNode cur = values.next();
        if (numNodes < maxInlinks) {
          nodeList.add((LinkNode)WritableUtils.clone(cur, conf));
          numNodes++;
        }
        else {
          break;
        }
      }

      LinkNode[] linkNodesAr = nodeList.toArray(new LinkNode[nodeList.size()]);
      LinkNodes linkNodes = new LinkNodes(linkNodesAr);
      output.collect(key, linkNodes);
    }

    public void close() {

    }
  }

  /**
   * Runs the inverter and merger jobs of the LinkDumper tool to create the 
   * url to inlink node database.
   */
  public void dumpLinks(Path webGraphDb)
    throws IOException {

    LOG.info("NodeDumper: starting");
    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    Path linkdump = new Path(webGraphDb, DUMP_DIR);
    Path nodeDb = new Path(webGraphDb, WebGraph.NODE_DIR);
    Path loopSetDb = new Path(webGraphDb, Loops.LOOPS_DIR);
    boolean loopsExists = fs.exists(loopSetDb);
    Path outlinkDb = new Path(webGraphDb, WebGraph.OUTLINK_DIR);

    // run the inverter job
    Path tempInverted = new Path(webGraphDb, "inverted-"
      + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
    JobConf inverter = new NutchJob(conf);
    inverter.setJobName("LinkDumper: inverter");
    FileInputFormat.addInputPath(inverter, nodeDb);
    if (loopsExists) {
      FileInputFormat.addInputPath(inverter, loopSetDb);
    }
    FileInputFormat.addInputPath(inverter, outlinkDb);
    inverter.setInputFormat(SequenceFileInputFormat.class);
    inverter.setMapperClass(Inverter.class);
    inverter.setReducerClass(Inverter.class);
    inverter.setMapOutputKeyClass(Text.class);
    inverter.setMapOutputValueClass(ObjectWritable.class);
    inverter.setOutputKeyClass(Text.class);
    inverter.setOutputValueClass(LinkNode.class);
    FileOutputFormat.setOutputPath(inverter, tempInverted);
    inverter.setOutputFormat(SequenceFileOutputFormat.class);

    try {
      LOG.info("LinkDumper: running inverter");
      JobClient.runJob(inverter);
      LOG.info("LinkDumper: finished inverter");
    }
    catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }

    // run the merger job
    JobConf merger = new NutchJob(conf);
    merger.setJobName("LinkDumper: merger");
    FileInputFormat.addInputPath(merger, tempInverted);
    merger.setInputFormat(SequenceFileInputFormat.class);
    merger.setReducerClass(Merger.class);
    merger.setMapOutputKeyClass(Text.class);
    merger.setMapOutputValueClass(LinkNode.class);
    merger.setOutputKeyClass(Text.class);
    merger.setOutputValueClass(LinkNodes.class);
    FileOutputFormat.setOutputPath(merger, linkdump);
    merger.setOutputFormat(MapFileOutputFormat.class);

    try {
      LOG.info("LinkDumper: running merger");
      JobClient.runJob(merger);
      LOG.info("LinkDumper: finished merger");
    }
    catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }

    fs.delete(tempInverted, true);
  }

  public static void main(String[] args)
    throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new LinkDumper(),
      args);
    System.exit(res);
  }

  /**
   * Runs the LinkDumper tool.  This simply creates the database, to read the
   * values the nested Reader tool must be used.
   */
  public int run(String[] args)
    throws Exception {

    Options options = new Options();
    Option helpOpts = OptionBuilder.withArgName("help").withDescription(
      "show this help message").create("help");
    Option webGraphDbOpts = OptionBuilder.withArgName("webgraphdb").hasArg()
      .withDescription("the web graph database to use").create("webgraphdb");
    options.addOption(helpOpts);
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
    }
    catch (Exception e) {
      LOG.fatal("LinkDumper: " + StringUtils.stringifyException(e));
      return -2;
    }
  }
}
