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
import java.text.SimpleDateFormat;
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
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.document.DateTools;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.scoring.webgraph.LinkDatum;
import org.apache.nutch.scoring.webgraph.Node;
import org.apache.nutch.scoring.webgraph.WebGraph;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.URLUtil;

/**
 * Creates the basic FieldWritable objects.  The basic fields are the main 
 * fields used in indexing segments.  Many other fields jobs will rely on the
 * urls being present in the basic fields output to create their fields for 
 * indexing.
 * 
 * Basic fields are extracted from segements.  Only urls that were successfully
 * fetched and parsed will be converted.  This job also implements a portion of
 * redirect logic.  If a url contains both a redirect or orig url then both the
 * url and its orig will be measured against their link analysis score with the
 * highest scoring one being the url used for display in the index.  This 
 * ensures that we index content under the best, most popular, url which is most
 * often the one users are expecting.
 * 
 * The BasicFields tool can accept one or more segments to convert to fields.
 * If multiple segments have overlapping content, only the latest successfully
 * fetched content will be converted.
 */
public class BasicFields
  extends Configured
  implements Tool {

  public static final Log LOG = LogFactory.getLog(BasicFields.class);

  /**
   * Runs the Extractor job. Extracts basic fields from segments.
   * 
   * @param nodeDb The node database
   * @param segment A single segment to process.
   * @param outputDir The extractor output.
   * 
   * @throws IOException If an error occurs while processing the segment.
   */
  private void runExtractor(Path nodeDb, Path segment, Path outputDir)
    throws IOException {

    LOG.info("BasicFields: starting extractor");
    JobConf job = new NutchJob(getConf());
    job.setJobName("BasicFields " + outputDir);

    LOG.info("BasicFields: extractor adding segment: " + segment);
    FileInputFormat.addInputPath(job, new Path(segment,
      CrawlDatum.FETCH_DIR_NAME));
    FileInputFormat.addInputPath(job, new Path(segment, ParseData.DIR_NAME));
    FileInputFormat.addInputPath(job, new Path(segment, ParseText.DIR_NAME));
    FileInputFormat.addInputPath(job, nodeDb);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(Extractor.class);
    job.setReducerClass(Extractor.class);
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ObjectWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FieldsWritable.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) {
      LOG.info("BasicFields: finished extractor");
    }
  }

  /**
   * Runs the Flipper job. Flipper is the first of a two part job to implement
   * redirect logic.
   * 
   * @param basicFields The basic fields temporary output.
   * @param nodeDb The node database.
   * @param outputDir The flipper output.
   * 
   * @throws IOException If an error occurs while processing.
   */
  private void runFlipper(Path basicFields, Path nodeDb, Path outputDir)
    throws IOException {

    LOG.info("BasicFields: starting flipper");
    JobConf job = new NutchJob(getConf());
    job.setJobName("BasicFields " + outputDir);
    FileInputFormat.addInputPath(job, nodeDb);
    FileInputFormat.addInputPath(job, basicFields);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(Flipper.class);
    job.setReducerClass(Flipper.class);
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ObjectWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LinkDatum.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) {
      LOG.info("BasicFields: finished flipper");
    }
  }

  /**
   * Runs the Scorer job. Scorer is the second of a two part job to implement
   * redirect logic.
   * 
   * @param basicFields The basic fields temporary output.
   * @param links The temporary output holding urls and any redirects.
   * @param outputDir The scorer output.
   * 
   * @throws IOException If an error occurs while processing.
   */
  private void runScorer(Path basicFields, Path links, Path outputDir)
    throws IOException {

    LOG.info("BasicFields: starting scorer");
    JobConf job = new NutchJob(getConf());
    job.setJobName("BasicFields " + outputDir);
    FileInputFormat.addInputPath(job, links);
    FileInputFormat.addInputPath(job, basicFields);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(Scorer.class);
    job.setReducerClass(Scorer.class);
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ObjectWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FieldsWritable.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) {
      LOG.info("BasicFields: finished scorer");
    }
  }

  /**
   * Runs the Merger job. Merger ensures that the most recent set of fields for
   * any given url is collected.
   * 
   * @param basicFields The basic fields final output.
   * @param outputDir The merger output.
   * 
   * @throws IOException If an error occurs while processing.
   */
  private void runMerger(Path[] basicFields, Path outputDir)
    throws IOException {

    LOG.info("BasicFields: starting merger");
    JobConf job = new NutchJob(getConf());
    job.setJobName("BasicFields " + outputDir);
    for (Path basic : basicFields) {
      FileInputFormat.addInputPath(job, basic);
    }
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setReducerClass(Merger.class);
    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FieldsWritable.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) {
      LOG.info("BasicFields: finished merger");
    }
  }

  /**
   * Extracts basic fields from a single segment.
   */
  private static class Extractor
    extends Configured
    implements Mapper<Text, Writable, Text, ObjectWritable>,
    Reducer<Text, ObjectWritable, Text, FieldsWritable> {

    private int MAX_TITLE_LENGTH;
    private Configuration conf;

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
     * Configures the job.
     */
    public void configure(JobConf conf) {
      this.conf = conf;
      this.MAX_TITLE_LENGTH = conf.getInt("indexer.max.title.length", 100);
    }

    public void close() {
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
     * Creates basic fields from a single segment.
     */
    public void reduce(Text key, Iterator<ObjectWritable> values,
      OutputCollector<Text, FieldsWritable> output, Reporter reporter)
      throws IOException {

      Node nodeDb = null;
      List<CrawlDatum> fetchDatums = new ArrayList<CrawlDatum>();
      ParseData parseData = null;
      ParseText parseText = null;
      List<FieldWritable> fieldsList = new ArrayList<FieldWritable>();

      // assign values, url must be successfully fetched and parsed
      while (values.hasNext()) {

        ObjectWritable objWrite = values.next();
        Object value = objWrite.get();
        if (value instanceof CrawlDatum) {
          CrawlDatum datum = (CrawlDatum)value;
          if (datum.getStatus() == CrawlDatum.STATUS_FETCH_SUCCESS) {
            fetchDatums.add(datum);
          }
        }
        else if (value instanceof Node) {
          nodeDb = (Node)value;
        }
        else if (value instanceof ParseData
          && ((ParseData)value).getStatus().isSuccess()) {
          parseData = (ParseData)value;
        }
        else if (value instanceof ParseText) {
          parseText = (ParseText)value;
        }
      }

      // if not successfully fetched and parsed then stop processing
      int numDatums = fetchDatums.size();
      if (numDatums == 0 || nodeDb == null || parseText == null
        || parseData == null) {
        return;
      }

      // get the most recent fetch time, this is duplicates inside of a single
      // segment, usually due to redirects
      CrawlDatum fetchDatum = null;
      long mostRecent = 0L;
      for (CrawlDatum cur : fetchDatums) {
        long fetchTime = cur.getFetchTime();
        if (fetchDatum == null || fetchTime > mostRecent) {
          fetchDatum = cur;
          mostRecent = fetchTime;
        }
      }

      // get parse metadata
      Metadata metadata = parseData.getContentMeta();
      Parse parse = new ParseImpl(parseText, parseData);

      // handle redirect urls
      Text reprUrlText = (Text)fetchDatum.getMetaData().get(
        Nutch.WRITABLE_REPR_URL_KEY);
      String reprUrl = reprUrlText != null ? reprUrlText.toString() : null;
      String url = key.toString();
      String fieldUrl = (reprUrl != null) ? reprUrl : url;
      String host = URLUtil.getHost(fieldUrl);

      // add segment, used to map from merged index back to segment files
      FieldWritable segField = new FieldWritable(Fields.SEGMENT,
        metadata.get(Nutch.SEGMENT_NAME_KEY), FieldType.CONTENT, false, true,
        false);
      fieldsList.add(segField);

      // add digest, used by dedup
      FieldWritable digestField = new FieldWritable(Fields.DIGEST,
        metadata.get(Nutch.SIGNATURE_KEY), FieldType.CONTENT, false, true,
        false);
      fieldsList.add(digestField);

      // url is both stored and indexed, so it's both searchable and returned
      fieldsList.add(new FieldWritable(Fields.URL, fieldUrl, FieldType.CONTENT,
        true, true, true));
      fieldsList.add(new FieldWritable(Fields.SEG_URL, url, FieldType.CONTENT,
        false, true, false));

      if (reprUrl != null) {
        // also store original url as both stored and indexes
        fieldsList.add(new FieldWritable(Fields.ORIG_URL, url,
          FieldType.CONTENT, true, true, true));
      }

      if (host != null) {
        // add host as un-stored, indexed and tokenized
        FieldWritable hostField = new FieldWritable(Fields.HOST, host,
          FieldType.CONTENT, true, false, true);
        fieldsList.add(hostField);

        // add site as un-stored, indexed and un-tokenized
        FieldWritable siteField = new FieldWritable(Fields.SITE, host,
          FieldType.CONTENT, true, false, false);
        fieldsList.add(siteField);
      }

      // content is indexed, so that it's searchable, but not stored in index
      fieldsList.add(new FieldWritable(Fields.CONTENT, parse.getText(),
        FieldType.CONTENT, true, false, true));

      // title
      String title = parse.getData().getTitle();
      if (title.length() > MAX_TITLE_LENGTH) { // truncate title if needed
        title = title.substring(0, MAX_TITLE_LENGTH);
      }
      // add title indexed and stored so that it can be displayed
      fieldsList.add(new FieldWritable(Fields.TITLE, title, FieldType.CONTENT,
        true, true, true));

      // add cached content/summary display policy, if available
      String caching = parse.getData().getMeta(Nutch.CACHING_FORBIDDEN_KEY);
      if (caching != null && !caching.equals(Nutch.CACHING_FORBIDDEN_NONE)) {
        fieldsList.add(new FieldWritable(Fields.CACHE, caching,
          FieldType.CONTENT, false, true, false));
      }

      // add timestamp when fetched, for deduplication
      fieldsList.add(new FieldWritable(Fields.TSTAMP, DateTools.timeToString(
        fetchDatum.getFetchTime(), DateTools.Resolution.MILLISECOND),
        FieldType.CONTENT, false, true, false));

      FieldsWritable fields = new FieldsWritable();
      fields.setFieldsList(fieldsList);
      output.collect(key, fields);
    }
  }

  /**
   * Runs the first part of redirect logic.  Breaks out fields if a page
   * contains a redirect.
   */
  public static class Flipper
    extends Configured
    implements Mapper<Text, Writable, Text, ObjectWritable>,
    Reducer<Text, ObjectWritable, Text, LinkDatum> {

    private JobConf conf;

    /**
     * Configures the job.
     */
    public void configure(JobConf conf) {
      this.conf = conf;
    }

    public void close() {
    }

    /**
     * Breaks out the collection of fields for url and redirects if necessary.
     */
    public void map(Text key, Writable value,
      OutputCollector<Text, ObjectWritable> output, Reporter reporter)
      throws IOException {

      ObjectWritable objUrl = new ObjectWritable();
      objUrl.set(key);

      if (value instanceof FieldsWritable) {

        // collect the fields for the url
        FieldsWritable fields = (FieldsWritable)value;
        FieldWritable url = fields.getField(Fields.URL);
        FieldWritable orig = fields.getField(Fields.ORIG_URL);
        output.collect(new Text(url.getValue()), objUrl);

        // collect for the orig / redirect url if one exists
        if (orig != null) {
          output.collect(new Text(orig.getValue()), objUrl);
        }
      }
      else {
        
        // anything else passes through
        ObjectWritable objWrite = new ObjectWritable();
        objWrite.set(value);
        output.collect(key, objWrite);
      }
    }

    /**
     * Collects redirect and original links for a given url key.  This will be
     * used in the Scorer to handle redirects.
     */
    public void reduce(Text key, Iterator<ObjectWritable> values,
      OutputCollector<Text, LinkDatum> output, Reporter reporter)
      throws IOException {

      Node node = null;
      List<String> urls = new ArrayList<String>();

      while (values.hasNext()) {
        ObjectWritable objWrite = values.next();
        Object obj = objWrite.get();
        if (obj instanceof Node) {
          node = (Node)obj;
        }
        else if (obj instanceof Text) {
          urls.add(obj.toString());
        }
      }

      if (urls.size() > 0) {
        float score = (node != null) ? node.getInlinkScore() : 0.0f;
        for (String url : urls) {
          LinkDatum datum = new LinkDatum(key.toString());
          datum.setScore(score);
          output.collect(new Text(url), datum);
        }
      }
    }
  }

  /**
   * The Scorer job sets the boost field from the NodeDb score.
   * 
   * It also runs the second part of redirect logic.  Determining the highest 
   * scoring url for pages that contain redirects.
   */
  public static class Scorer
    extends Configured
    implements Mapper<Text, Writable, Text, ObjectWritable>,
    Reducer<Text, ObjectWritable, Text, FieldsWritable> {

    private JobConf conf;

    /**
     * Configures the job.
     */
    public void configure(JobConf conf) {
      this.conf = conf;
    }

    public void close() {
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
     * Sets a document boost field from the NodeDb and determines the best 
     * scoring url for pages that have rediects.  Uses the highest scoring url 
     * as the display url in the index.
     */
    public void reduce(Text key, Iterator<ObjectWritable> values,
      OutputCollector<Text, FieldsWritable> output, Reporter reporter)
      throws IOException {

      FieldsWritable fields = null;
      List<LinkDatum> datums = new ArrayList<LinkDatum>();

      while (values.hasNext()) {
        ObjectWritable objWrite = values.next();
        Object obj = objWrite.get();
        if (obj instanceof FieldsWritable) {
          fields = (FieldsWritable)obj;
        }
        else if (obj instanceof LinkDatum) {
          datums.add((LinkDatum)obj);
        }
      }

      int numDatums = datums.size();
      if (fields != null && numDatums > 0) {

        // if no redirect for the page just assign the linkrank boost
        List<FieldWritable> fieldsList = fields.getFieldsList();
        if (numDatums == 1) {
          float linkRank = datums.get(0).getScore();
          fieldsList.add(new FieldWritable(Fields.BOOST, "linkrank",
            FieldType.BOOST, linkRank));
          output.collect(new Text(key), fields);
        }
        else {

          // get both the url and any rediect url stored
          FieldWritable url = fields.getField(Fields.URL);
          FieldWritable orig = fields.getField(Fields.ORIG_URL);
          float urlScore = 0.0f;
          float origScore = 0.0f;

          // get the scores for each
          for (LinkDatum datum : datums) {
            String curUrl = datum.getUrl();
            if (curUrl.equals(url.getValue())) {
              urlScore = datum.getScore();
            }
            else if (curUrl.equals(orig.getValue())) {
              origScore = datum.getScore();
            }
          }

          // if the highest scoring url is not the one currently displayed in 
          // the index under the current basic fields, then switch it
          String urlKey = url.getValue();
          float linkRank = urlScore;
          if (origScore > urlScore) {
            url.setName(Fields.ORIG_URL);
            orig.setName(Fields.URL);

            // We also need to fix the host because we are changing urls
            String host = URLUtil.getHost(orig.getValue());
            if (host != null) {
              fieldsList.remove(fields.getField(Fields.SITE));
              fieldsList.remove(fields.getField(Fields.HOST));
              fieldsList.add(new FieldWritable(Fields.HOST, host,
                FieldType.CONTENT, true, false, true));
              fieldsList.add(new FieldWritable(Fields.SITE, host,
                FieldType.CONTENT, true, false, false));
            }

            linkRank = origScore;
            urlKey = orig.getValue();
          }

          // create the final document boost field
          fieldsList.add(new FieldWritable(Fields.BOOST, "linkrank",
            FieldType.BOOST, linkRank));
          output.collect(new Text(urlKey), fields);
        }
      }
    }
  }

  /**
   * Merges output of all segments fields collecting only the most recent set
   * of fields for any given url.
   */
  public static class Merger
    extends Configured
    implements Reducer<Text, FieldsWritable, Text, FieldsWritable> {

    private JobConf conf;

    /**
     * Configures the job.
     */
    public void configure(JobConf conf) {
      this.conf = conf;
    }

    public void close() {
    }

    /**
     * Collects the most recent set of fields for any url.
     */
    public void reduce(Text key, Iterator<FieldsWritable> values,
      OutputCollector<Text, FieldsWritable> output, Reporter reporter)
      throws IOException {

      List<FieldsWritable> fields = new ArrayList<FieldsWritable>();

      // collects the various sets of fields
      while (values.hasNext()) {
        fields.add((FieldsWritable)WritableUtils.clone(values.next(), conf));
      }

      // if only one set of fields for a given url passthrough
      FieldsWritable outFields = null;
      int numFields = fields.size();
      if (numFields == 1) {
        outFields = fields.get(0);
      }
      else if (numFields > 1) {

        // more than one set of fields means url has been fetched more than 
        // once, collect only the most recent set of fields
        FieldsWritable mostRecent = null;
        long recentTime = 0L;
        for (int i = 0; i < numFields; i++) {
          FieldsWritable cur = fields.get(i);
          String tStampStr = cur.getField(Fields.TSTAMP).getValue();
          long timestamp = Long.parseLong(tStampStr);
          if (mostRecent == null || recentTime < timestamp) {
            recentTime = timestamp;
            mostRecent = cur;
          }
        }

        outFields = mostRecent;
      }

      output.collect(key, outFields);
    }
  }

  /**
   * Runs the BasicFields jobs for every segment and aggregates and filters 
   * the output to create a final database of FieldWritable objects.
   * 
   * @param nodeDb The node database.
   * @param segments The array of segments to process.
   * @param output The BasicFields output.
   * 
   * @throws IOException If an error occurs while processing the segments.
   */
  public void createFields(Path nodeDb, Path[] segments, Path output)
    throws IOException {

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("BasicFields: starting at " + sdf.format(start));

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    Path tempOutput = new Path(output.toString() + "-temp");
    fs.mkdirs(tempOutput);
    int numSegments = segments.length;
    Path[] basicFields = new Path[numSegments];

    // one pass per segment to extract and create the basic fields
    for (int i = 0; i < numSegments; i++) {

      Path segment = segments[i];
      Path segOutput = new Path(tempOutput, String.valueOf(i));
      Path tempBasic = new Path(tempOutput, "basic-"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
      Path tempFlip = new Path(tempOutput, "flip-"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

      runExtractor(nodeDb, segment, tempBasic);
      runFlipper(tempBasic, nodeDb, tempFlip);
      runScorer(tempBasic, tempFlip, segOutput);

      fs.delete(tempBasic, true);
      fs.delete(tempFlip, true);
      basicFields[i] = segOutput;
    }

    // merge all of the segments and delete any temporary output
    runMerger(basicFields, output);
    fs.delete(tempOutput, true);
    long end = System.currentTimeMillis();
    LOG.info("BasicFields: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args)
    throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new BasicFields(),
      args);
    System.exit(res);
  }

  /**
   * Runs the BasicFields tool.
   */
  public int run(String[] args)
    throws Exception {

    Options options = new Options();
    Option helpOpts = OptionBuilder.withArgName("help").withDescription(
      "show this help message").create("help");
    Option outputOpts = OptionBuilder.withArgName("output").hasArg().withDescription(
      "the output index directory").create("output");
    Option webGraphOpts = OptionBuilder.withArgName("webgraphdb").hasArg().withDescription(
      "the webgraphdb to use").create("webgraphdb");
    Option segOpts = OptionBuilder.withArgName("segment").hasArgs().withDescription(
      "the segment(s) to use").create("segment");
    options.addOption(helpOpts);
    options.addOption(webGraphOpts);
    options.addOption(segOpts);
    options.addOption(outputOpts);

    CommandLineParser parser = new GnuParser();
    try {

      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("webgraphdb")
        || !line.hasOption("output") || !line.hasOption("segment")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("BasicFields", options);
        return -1;
      }

      // get the command line options and all of the segments
      String webGraphDb = line.getOptionValue("webgraphdb");
      String output = line.getOptionValue("output");
      String[] segments = line.getOptionValues("segment");
      Path[] segPaths = new Path[segments.length];
      for (int i = 0; i < segments.length; i++) {
        segPaths[i] = new Path(segments[i]);
      }

      createFields(new Path(webGraphDb, WebGraph.NODE_DIR), segPaths, new Path(
        output));
      return 0;
    }
    catch (Exception e) {
      LOG.fatal("BasicFields: " + StringUtils.stringifyException(e));
      return -2;
    }
  }
}
