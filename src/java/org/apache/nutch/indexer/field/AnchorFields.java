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
package org.apache.nutch.indexer.field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.scoring.webgraph.LinkDatum;
import org.apache.nutch.scoring.webgraph.Node;
import org.apache.nutch.scoring.webgraph.WebGraph;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/**
 * Creates FieldWritable objects for inbound anchor text.   These FieldWritable
 * objects are then included in the input to the FieldIndexer to be converted
 * to Lucene Field objects and indexed.
 * 
 * Any empty or null anchor text is ignored. Anchors are sorted in descending
 * order according to the score of their parent pages. There are settings for a
 * maximum number of anchors to index and whether those anchors should be stored
 * and tokenized. With a descending order by score and a maximum anchors index
 * we ensure that only the best anchors are indexed assuming that a higher link
 * analysis score equals a better page and better inbound text.
 */
public class AnchorFields
  extends Configured
  implements Tool {

  public static final Log LOG = LogFactory.getLog(AnchorFields.class);

  /**
   * Comparator to order the links in descending order by score.
   */
  private static class DescendinLinkDatumScoreComparator
    implements Comparator<LinkDatum> {

    public int compare(LinkDatum one, LinkDatum two) {
      float scoreOne = one.getScore();
      float scoreTwo = two.getScore();
      return (scoreOne == scoreTwo ? 0 : (scoreOne > scoreTwo ? -1 : 1));
    }
  }

  /**
   * Runs the Extractor job.  Get outlinks to be converted while ignoring empty
   * and null anchors.
   * 
   * @param webGraphDb The WebGraphDb to pull from.
   * @param output The extractor output.
   * 
   * @throws IOException If an error occurs while running the extractor.
   */
  private void runExtractor(Path webGraphDb, Path output)
    throws IOException {

    JobConf extractor = new NutchJob(getConf());
    extractor.setJobName("AnchorFields Extractor");
    FileInputFormat.addInputPath(extractor, new Path(webGraphDb,
      WebGraph.OUTLINK_DIR));
    FileInputFormat.addInputPath(extractor, new Path(webGraphDb,
      WebGraph.NODE_DIR));
    FileOutputFormat.setOutputPath(extractor, output);
    extractor.setInputFormat(SequenceFileInputFormat.class);
    extractor.setMapperClass(Extractor.class);
    extractor.setReducerClass(Extractor.class);
    extractor.setMapOutputKeyClass(Text.class);
    extractor.setMapOutputValueClass(ObjectWritable.class);
    extractor.setOutputKeyClass(Text.class);
    extractor.setOutputValueClass(LinkDatum.class);
    extractor.setOutputFormat(SequenceFileOutputFormat.class);

    LOG.info("Starting extractor job");
    try {
      JobClient.runJob(extractor);
    }
    catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }
    LOG.info("Finished extractor job.");
  }

  /**
   * Runs the collector job.  Aggregates extracted inlinks, sorts and converts
   * the highest scoring into FieldWritable objects.  Only inlinks for which
   * basic fields exist will be collected to avoid orphan fields.
   * 
   * @param basicFields The BasicFields which must be present to collect anchors
   * to avoid orphan fields.
   * @param links The outlinks path.
   * @param output The collector output.
   * 
   * @throws IOException If an error occurs while running the collector.
   */
  private void runCollector(Path basicFields, Path links, Path output)
    throws IOException {

    JobConf collector = new NutchJob(getConf());
    collector.setJobName("AnchorFields Collector");
    FileInputFormat.addInputPath(collector, links);
    FileInputFormat.addInputPath(collector, basicFields);
    FileOutputFormat.setOutputPath(collector, output);
    collector.setInputFormat(SequenceFileInputFormat.class);
    collector.setMapOutputKeyClass(Text.class);
    collector.setMapOutputValueClass(ObjectWritable.class);
    collector.setMapperClass(Collector.class);
    collector.setReducerClass(Collector.class);
    collector.setOutputKeyClass(Text.class);
    collector.setOutputValueClass(FieldWritable.class);
    collector.setOutputFormat(SequenceFileOutputFormat.class);

    LOG.info("Starting collector job");
    try {
      JobClient.runJob(collector);
    }
    catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }
    LOG.info("Finished collector job.");
  }

  /**
   * Extracts outlinks to be created as FieldWritable objects.  Ignores empty
   * and null anchors.
   */
  public static class Extractor
    extends Configured
    implements Mapper<Text, Writable, Text, ObjectWritable>,
    Reducer<Text, ObjectWritable, Text, LinkDatum> {

    private boolean ignoreEmptyAnchors = true;
    private JobConf conf;

    /**
     * Default constructor.
     */
    public Extractor() {
    }

    /**
     * Configurable constructor.
     */
    public Extractor(Configuration conf) {
      setConf(conf);
    }

    /**
     * Configures the job, sets to ignore empty anchors.
     */
    public void configure(JobConf conf) {
      this.conf = conf;
      ignoreEmptyAnchors = conf.getBoolean("link.ignore.empty.anchors", true);
    }

    /**
     * Wraps values in ObjectWritable
     */
    public void map(Text key, Writable value,
      OutputCollector<Text, ObjectWritable> output, Reporter reporter)
      throws IOException {

      ObjectWritable objWrite = new ObjectWritable();
      objWrite.set(value);
      output.collect(key, objWrite);
    }

    /**
     * Extracts and inverts outlinks, ignores empty anchors.
     */
    public void reduce(Text key, Iterator<ObjectWritable> values,
      OutputCollector<Text, LinkDatum> output, Reporter reporter)
      throws IOException {

      List<LinkDatum> outlinkList = new ArrayList<LinkDatum>();
      Node node = null;

      // collect the outlinks while ignoring links with empty anchor text, also
      // assign the node
      while (values.hasNext()) {
        ObjectWritable objWrite = values.next();
        Object obj = objWrite.get();
        if (obj instanceof LinkDatum) {
          LinkDatum next = (LinkDatum)obj;
          String anchor = next.getAnchor();
          if (anchor != null) {
            anchor = anchor.trim();
          }
          if (ignoreEmptyAnchors && (anchor == null || anchor.length() == 0)) {
            continue;
          }
          outlinkList.add(next);
        }
        else if (obj instanceof Node) {
          node = (Node)obj;
        }
      }

      // has to have outlinks to index
      if (node != null && outlinkList.size() > 0) {
        String fromUrl = key.toString();
        float outlinkScore = node.getInlinkScore();
        for (LinkDatum datum : outlinkList) {
          String toUrl = datum.getUrl();
          datum.setUrl(fromUrl);
          datum.setScore(outlinkScore);
          datum.setLinkType(LinkDatum.INLINK);
          output.collect(new Text(toUrl), datum);
        }
      }
    }

    public void close() {
    }
  }

  /**
   * Collects and creates FieldWritable objects from the inlinks. Inlinks are
   * first sorted by descending score before being collected.
   */
  public static class Collector
    extends Configured
    implements Mapper<Text, Writable, Text, ObjectWritable>,
    Reducer<Text, ObjectWritable, Text, FieldWritable> {

    private int maxInlinks = 1000;
    private boolean tokenize = true;
    private boolean stored = false;
    private Comparator<LinkDatum> descLinkComp = new DescendinLinkDatumScoreComparator();

    /**
     * Configures the jobs. Sets maximum number of inlinks and whether to
     * tokenize and store.
     */
    public void configure(JobConf conf) {
      this.maxInlinks = conf.getInt("link.max.inlinks", 1000);
      this.tokenize = conf.getBoolean("indexer.anchor.tokenize", true);
      this.stored = conf.getBoolean("indexer.anchor.stored", false);
    }

    public void close() {
    }

    /**
     * Wraps values in ObjectWritable
     */
    public void map(Text key, Writable value,
      OutputCollector<Text, ObjectWritable> output, Reporter reporter)
      throws IOException {

      ObjectWritable objWrite = new ObjectWritable();
      objWrite.set(value);
      output.collect(key, objWrite);
    }

    /**
     * Aggregates and sorts inlinks. Then converts up to a max number to
     * FieldWritable objects.
     */
    public void reduce(Text key, Iterator<ObjectWritable> values,
      OutputCollector<Text, FieldWritable> output, Reporter reporter)
      throws IOException {

      List<LinkDatum> anchors = new ArrayList<LinkDatum>();
      FieldsWritable basicFields = null;

      // aggregate inlinks assign basic fields
      while (values.hasNext()) {
        ObjectWritable objWrite = values.next();
        Object obj = objWrite.get();
        if (obj instanceof LinkDatum) {
          anchors.add((LinkDatum)obj);
        }
        else if (obj instanceof FieldsWritable) {
          basicFields = (FieldsWritable)obj;
        }
      }

      // only collect anchors for those urls that have basic fields, otherwise
      // we get orphan entries indexed only under anchor text
      if (basicFields != null && anchors.size() > 0) {

        // sort according to score descending
        Collections.sort(anchors, descLinkComp);

        // collect to maximum number of inlinks
        int numToCollect = (maxInlinks > anchors.size() ? anchors.size()
          : maxInlinks);
        for (int i = 0; i < numToCollect; i++) {
          LinkDatum datum = anchors.get(i);
          FieldWritable anchorField = new FieldWritable(Fields.ANCHOR,
            datum.getAnchor(), FieldType.CONTENT, true, stored, tokenize);
          output.collect(key, anchorField);
        }
      }
    }
  }

  /**
   * Creates the FieldsWritable object from the anchors.
   * 
   * @param webGraphDb The WebGraph from which to pull outlinks.
   * @param basicFields The BasicFields that must be present to avoid orphan
   * anchor fields.
   * @param output The AnchorFields output.
   * 
   * @throws IOException If an error occurs while creating the fields.
   */
  public void createFields(Path webGraphDb, Path basicFields, Path output)
    throws IOException {

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    Path tempLinks = new Path(output + "-"
      + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
    runExtractor(webGraphDb, tempLinks);
    runCollector(basicFields, tempLinks, output);
    fs.delete(tempLinks, true);
  }

  public static void main(String[] args)
    throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new AnchorFields(),
      args);
    System.exit(res);
  }

  /**
   * Runs the AnchorFields job.
   */
  public int run(String[] args)
    throws Exception {

    Options options = new Options();
    Option helpOpts = OptionBuilder.withArgName("help").withDescription(
      "show this help message").create("help");
    Option outputOpts = OptionBuilder.withArgName("output").hasArg().withDescription(
      "the output index directory").create("output");
    Option webGraphDbOpts = OptionBuilder.withArgName("webgraphdb").hasArg().withDescription(
      "the webgraphdb to use").create("webgraphdb");
    Option basicFieldOpts = OptionBuilder.withArgName("basicfields").hasArgs().withDescription(
      "the basicfields to use").create("basicfields");
    options.addOption(helpOpts);
    options.addOption(webGraphDbOpts);
    options.addOption(basicFieldOpts);
    options.addOption(outputOpts);

    CommandLineParser parser = new GnuParser();
    try {

      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("webgraphdb")
        || !line.hasOption("output") || !line.hasOption("basicfields")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("AnchorFields", options);
        return -1;
      }

      String webGraphDb = line.getOptionValue("webgraphdb");
      String output = line.getOptionValue("output");
      String basicFields = line.getOptionValue("basicfields");

      createFields(new Path(webGraphDb), new Path(basicFields),
        new Path(output));
      return 0;
    }
    catch (Exception e) {
      LOG.fatal("AnchorFields: " + StringUtils.stringifyException(e));
      return -2;
    }
  }
}
