/**
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
package org.apache.nutch.tools.arc;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.fetcher.FetcherOutputFormat;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.StringUtil;

/**
 * <p>The <code>ArcSegmentCreator</code> is a replacement for fetcher that will
 * take arc files as input and produce a nutch segment as output.</p>
 * 
 * <p>Arc files are tars of compressed gzips which are produced by both the
 * internet archive project and the grub distributed crawler project.</p>
 * 
 */
public class ArcSegmentCreator
  extends Configured
  implements Tool, Mapper<Text, BytesWritable, Text, NutchWritable> {

  public static final Log LOG = LogFactory.getLog(ArcSegmentCreator.class);
  public static final String URL_VERSION = "arc.url.version";
  private JobConf jobConf;
  private URLFilters urlFilters;
  private ScoringFilters scfilters;
  private ParseUtil parseUtil;
  private URLNormalizers normalizers;
  private int interval;

  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

  public ArcSegmentCreator() {

  }

  /**
   * <p>Constructor that sets the job configuration.</p>
   * 
   * @param conf
   */
  public ArcSegmentCreator(Configuration conf) {
    setConf(conf);
  }

  /**
   * Generates a random name for the segments.
   * 
   * @return The generated segment name.
   */
  public static synchronized String generateSegmentName() {
    try {
      Thread.sleep(1000);
    }
    catch (Throwable t) {
    }
    return sdf.format(new Date(System.currentTimeMillis()));
  }

  /**
   * <p>Configures the job.  Sets the url filters, scoring filters, url normalizers
   * and other relevant data.</p>
   * 
   * @param job The job configuration.
   */
  public void configure(JobConf job) {

    // set the url filters, scoring filters the parse util and the url
    // normalizers
    this.jobConf = job;
    this.urlFilters = new URLFilters(jobConf);
    this.scfilters = new ScoringFilters(jobConf);
    this.parseUtil = new ParseUtil(jobConf);
    this.normalizers = new URLNormalizers(jobConf, URLNormalizers.SCOPE_FETCHER);
    interval = jobConf.getInt("db.fetch.interval.default", 2592000);
  }

  public void close() {
  }

  /**
   * <p>Parses the raw content of a single record to create output.  This method
   * is almost the same as the {@link org.apache.nutch.Fetcher#output} method in
   * terms of processing and output.  
   * 
   * @param output  The job output collector.
   * @param segmentName The name of the segment to create.
   * @param key The url of the record.
   * @param datum The CrawlDatum of the record.
   * @param content The raw content of the record
   * @param pstatus The protocol status
   * @param status The fetch status.
   * 
   * @return The result of the parse in a ParseStatus object.
   */
  private ParseStatus output(OutputCollector<Text, NutchWritable> output, String segmentName,
    Text key, CrawlDatum datum, Content content, ProtocolStatus pstatus,
    int status) {

    // set the fetch status and the fetch time
    datum.setStatus(status);
    datum.setFetchTime(System.currentTimeMillis());
    if (pstatus != null)
      datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY, pstatus);

    ParseResult parseResult = null;
    if (content != null) {
      Metadata metadata = content.getMetadata();
      // add segment to metadata
      metadata.set(Nutch.SEGMENT_NAME_KEY, segmentName);
      // add score to content metadata so that ParseSegment can pick it up.
      try {
        scfilters.passScoreBeforeParsing(key, datum, content);
      }
      catch (Exception e) {
        if (LOG.isWarnEnabled()) {
          e.printStackTrace(LogUtil.getWarnStream(LOG));
          LOG.warn("Couldn't pass score, url " + key + " (" + e + ")");
        }
      }

      try {

        // parse the content
        parseResult = this.parseUtil.parse(content);
      }
      catch (Exception e) {
        LOG.warn("Error parsing: " + key + ": "
          + StringUtils.stringifyException(e));
      }

      // set the content signature
      if (parseResult == null) {
        byte[] signature = SignatureFactory.getSignature(getConf()).calculate(
          content, new ParseStatus().getEmptyParse(getConf()));
        datum.setSignature(signature);
      }

      try {
        output.collect(key, new NutchWritable(datum));
        output.collect(key, new NutchWritable(content));

        if (parseResult != null) {
          for (Entry <Text, Parse> entry : parseResult) {
            Text url = entry.getKey();
            Parse parse = entry.getValue();
            ParseStatus parseStatus = parse.getData().getStatus();

            if (!parseStatus.isSuccess()) {
              LOG.warn("Error parsing: " + key + ": " + parseStatus);
              parse = parseStatus.getEmptyParse(getConf());
            }

            // Calculate page signature. 
            byte[] signature = SignatureFactory.getSignature(getConf()).calculate(
              content, parse);
            // Ensure segment name and score are in parseData metadata
            parse.getData().getContentMeta().set(Nutch.SEGMENT_NAME_KEY,
              segmentName);
            parse.getData().getContentMeta().set(Nutch.SIGNATURE_KEY,
              StringUtil.toHexString(signature));
            // Pass fetch time to content meta
            parse.getData().getContentMeta().set(Nutch.FETCH_TIME_KEY,
              Long.toString(datum.getFetchTime()));
            if (url.equals(key))
              datum.setSignature(signature);
            try {
              scfilters.passScoreAfterParsing(url, content, parse);
            }
            catch (Exception e) {
              if (LOG.isWarnEnabled()) {
                e.printStackTrace(LogUtil.getWarnStream(LOG));
                LOG.warn("Couldn't pass score, url " + key + " (" + e + ")");
              }
            }
            output.collect(url, new NutchWritable(new ParseImpl(new ParseText(
              parse.getText()), parse.getData(), parse.isCanonical())));
          }
        }
      }
      catch (IOException e) {
        if (LOG.isFatalEnabled()) {
          LOG.fatal("ArcSegmentCreator caught:" + StringUtils.stringifyException(e));
        }
      }

      // return parse status if it exits
      if (parseResult != null && !parseResult.isEmpty()) {
        Parse p = parseResult.get(content.getUrl());
        if (p != null) {
          return p.getData().getStatus();
        }
      }
    }
    
    return null;
  }

  /**
   * <p>Logs any error that occurs during conversion.</p>
   * 
   * @param url The url we are parsing.
   * @param t The error that occured.
   */
  private void logError(Text url, Throwable t) {
    if (LOG.isInfoEnabled()) {
      LOG.info("Conversion of " + url + " failed with: " + 
          StringUtils.stringifyException(t));
    }
  }

  /**
   * <p>Runs the Map job to translate an arc record into output for Nutch 
   * segments.</p>
   * 
   * @param key The arc record header.
   * @param bytes The arc record raw content bytes.
   * @param output The output collecter.
   * @param reporter The progress reporter.
   */
  public void map(Text key, BytesWritable bytes,
    OutputCollector<Text, NutchWritable> output, Reporter reporter)
    throws IOException {

    String[] headers = key.toString().split("\\s+");
    String urlStr = headers[0];
    String version = headers[2];
    String contentType = headers[3];
    
    // arcs start with a file description.  for now we ignore this as it is not
    // a content record
    if (urlStr.startsWith("filedesc://")) {
      LOG.info("Ignoring file header: " + urlStr);
      return;
    }
    LOG.info("Processing: " + urlStr);

    // get the raw  bytes from the arc file, create a new crawldatum
    Text url = new Text();
    CrawlDatum datum = new CrawlDatum(CrawlDatum.STATUS_DB_FETCHED, interval,
      1.0f);
    String segmentName = getConf().get(Nutch.SEGMENT_NAME_KEY);

    // normalize and filter the urls
    try {
      urlStr = normalizers.normalize(urlStr, URLNormalizers.SCOPE_FETCHER);
      urlStr = urlFilters.filter(urlStr); // filter the url
    }
    catch (Exception e) {
      if (LOG.isWarnEnabled()) {
        LOG.warn("Skipping " + url + ":" + e);
      }
      urlStr = null;
    }

    // if still a good url then process
    if (urlStr != null) {

      url.set(urlStr);
      try {

        // set the protocol status to success and the crawl status to success
        // create the content from the normalized url and the raw bytes from
        // the arc file,  TODO: currently this doesn't handle text of errors
        // pages (i.e. 404, etc.). We assume we won't get those.
        ProtocolStatus status = ProtocolStatus.STATUS_SUCCESS;
        Content content = new Content(urlStr, urlStr, bytes.get(), contentType,
          new Metadata(), getConf());
        
        // set the url version into the metadata
        content.getMetadata().set(URL_VERSION, version);
        ParseStatus pstatus = null;
        pstatus = output(output, segmentName, url, datum, content, status,
          CrawlDatum.STATUS_FETCH_SUCCESS);
        reporter.progress();
      }
      catch (Throwable t) { // unexpected exception
        logError(url, t);
        output(output, segmentName, url, datum, null, null,
          CrawlDatum.STATUS_FETCH_RETRY);
      }
    }
  }

  /**
   * <p>Creates the arc files to segments job.</p>
   * 
   * @param arcFiles The path to the directory holding the arc files
   * @param segmentsOutDir The output directory for writing the segments
   * 
   * @throws IOException If an IO error occurs while running the job.
   */
  public void createSegments(Path arcFiles, Path segmentsOutDir)
    throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("ArcSegmentCreator: starting");
      LOG.info("ArcSegmentCreator: arc files dir: " + arcFiles);
    }

    JobConf job = new NutchJob(getConf());
    job.setJobName("ArcSegmentCreator " + arcFiles);
    String segName = generateSegmentName();
    job.set(Nutch.SEGMENT_NAME_KEY, segName);
    job.setInputPath(arcFiles);
    job.setInputFormat(ArcInputFormat.class);
    job.setMapperClass(ArcSegmentCreator.class);
    job.setOutputPath(new Path(segmentsOutDir, segName));
    job.setOutputFormat(FetcherOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NutchWritable.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) {
      LOG.info("ArcSegmentCreator: done");
    }
  }

  public static void main(String args[])
    throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new ArcSegmentCreator(), args);
    System.exit(res);
  }

  public int run(String[] args)
    throws Exception {

    String usage = "Usage: ArcSegmentCreator <arcFiles> <segmentsOutDir>";

    if (args.length < 2) {
      System.err.println(usage);
      return -1;
    }

    // set the arc files directory and the segments output directory
    Path arcFiles = new Path(args[0]);
    Path segmentsOutDir = new Path(args[1]);

    try {
      // create the segments from the arc files
      createSegments(arcFiles, segmentsOutDir);
      return 0;
    }
    catch (Exception e) {
      LOG.fatal("ArcSegmentCreator: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
}
