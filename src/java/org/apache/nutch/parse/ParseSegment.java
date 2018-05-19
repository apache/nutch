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

package org.apache.nutch.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.segment.SegmentChecker;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TimingUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/* Parse content in a segment. */
public class ParseSegment extends NutchTool implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static final String SKIP_TRUNCATED = "parser.skip.truncated";

  public ParseSegment() {
    this(null);
  }

  public ParseSegment(Configuration conf) {
    super(conf);
  }

  public static class ParseSegmentMapper extends
     Mapper<WritableComparable<?>, Content, Text, ParseImpl> {

    private ParseUtil parseUtil;
    private Text newKey = new Text();
    private ScoringFilters scfilters;
    private boolean skipTruncated;

    @Override
    public void setup(Mapper<WritableComparable<?>, Content, Text, ParseImpl>.Context context) {
      Configuration conf = context.getConfiguration();
      scfilters = new ScoringFilters(conf);
      skipTruncated = conf.getBoolean(SKIP_TRUNCATED, true);
    }

    @Override
    public void cleanup(Context context){
    }

    @Override
    public void map(WritableComparable<?> key, Content content,
        Context context)
        throws IOException, InterruptedException {
      // convert on the fly from old UTF8 keys
      if (key instanceof Text) {
        newKey.set(key.toString());
        key = newKey;
      }

      String fetchStatus = content.getMetadata().get(Nutch.FETCH_STATUS_KEY);
      if (fetchStatus == null) {
        // no fetch status, skip document
        LOG.debug("Skipping {} as content has no fetch status", key);
        return;
      } else if (Integer.parseInt(fetchStatus) != CrawlDatum.STATUS_FETCH_SUCCESS) {
        // content not fetched successfully, skip document
        LOG.debug("Skipping {} as content is not fetched successfully", key);
        return;
      }

      if (skipTruncated && isTruncated(content)) {
        return;
      }

      long start = System.currentTimeMillis();
      ParseResult parseResult = null;
      try {
        if (parseUtil == null)
          parseUtil = new ParseUtil(context.getConfiguration());
        parseResult = parseUtil.parse(content);
      } catch (Exception e) {
        LOG.warn("Error parsing: " + key + ": "
            + StringUtils.stringifyException(e));
        return;
      }

      for (Entry<Text, Parse> entry : parseResult) {
        Text url = entry.getKey();
        Parse parse = entry.getValue();
        ParseStatus parseStatus = parse.getData().getStatus();

        context.getCounter("ParserStatus",
            ParseStatus.majorCodes[parseStatus.getMajorCode()]).increment(1);

        if (!parseStatus.isSuccess()) {
          LOG.warn("Error parsing: " + key + ": " + parseStatus);
          parse = parseStatus.getEmptyParse(context.getConfiguration());
        }

        // pass segment name to parse data
        parse.getData().getContentMeta()
            .set(Nutch.SEGMENT_NAME_KEY, context.getConfiguration().get(Nutch.SEGMENT_NAME_KEY));

        // compute the new signature
        byte[] signature = SignatureFactory.getSignature(context.getConfiguration()).calculate(
            content, parse);
        parse.getData().getContentMeta()
            .set(Nutch.SIGNATURE_KEY, StringUtil.toHexString(signature));

        try {
          scfilters.passScoreAfterParsing(url, content, parse);
        } catch (ScoringFilterException e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Error passing score: " + url + ": " + e.getMessage());
          }
        }

        long end = System.currentTimeMillis();
        LOG.info("Parsed (" + Long.toString(end - start) + "ms):" + url);

        context.write(
            url,
            new ParseImpl(new ParseText(parse.getText()), parse.getData(), parse
                .isCanonical()));
      }
    }
  }

  /**
   * Checks if the page's content is truncated.
   * 
   * @param content
   * @return If the page is truncated <code>true</code>. When it is not, or when
   *         it could be determined, <code>false</code>.
   */
  public static boolean isTruncated(Content content) {
    byte[] contentBytes = content.getContent();
    if (contentBytes == null)
      return false;
    Metadata metadata = content.getMetadata();
    if (metadata == null)
      return false;

    String lengthStr = metadata.get(Response.CONTENT_LENGTH);
    if (lengthStr != null)
      lengthStr = lengthStr.trim();
    if (StringUtil.isEmpty(lengthStr)) {
      return false;
    }
    int inHeaderSize;
    String url = content.getUrl();
    try {
      inHeaderSize = Integer.parseInt(lengthStr);
    } catch (NumberFormatException e) {
      LOG.warn("Wrong contentlength format for " + url, e);
      return false;
    }
    int actualSize = contentBytes.length;
    if (inHeaderSize > actualSize) {
      LOG.info(url + " skipped. Content of size " + inHeaderSize
          + " was truncated to " + actualSize);
      return true;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(url + " actualSize=" + actualSize + " inHeaderSize="
          + inHeaderSize);
    }
    return false;
  }

  public static class ParseSegmentReducer extends
     Reducer<Text, Writable, Text, Writable> {

    @Override
    public void reduce(Text key, Iterable<Writable> values,
        Context context)
        throws IOException, InterruptedException {
      Iterator<Writable> valuesIter = values.iterator();
      context.write(key, valuesIter.next()); // collect first value
    }
  }

  public void parse(Path segment) throws IOException, 
      InterruptedException, ClassNotFoundException {
    if (SegmentChecker.isParsed(segment, segment.getFileSystem(getConf()))) {
      LOG.warn("Segment: " + segment
          + " already parsed!! Skipped parsing this segment!!"); // NUTCH-1854
      return;
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("ParseSegment: starting at {}", sdf.format(start));
      LOG.info("ParseSegment: segment: {}", segment);
    }

    Job job = NutchJob.getInstance(getConf());
    job.setJobName("parse " + segment);

    Configuration conf = job.getConfiguration();
    FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));
    conf.set(Nutch.SEGMENT_NAME_KEY, segment.getName());
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setJarByClass(ParseSegment.class);
    job.setMapperClass(ParseSegment.ParseSegmentMapper.class);
    job.setReducerClass(ParseSegment.ParseSegmentReducer.class);

    FileOutputFormat.setOutputPath(job, segment);
    job.setOutputFormatClass(ParseOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ParseImpl.class);

    try{
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "Parse job did not succeed, job status:"
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    }

    long end = System.currentTimeMillis();
    LOG.info("ParseSegment: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new ParseSegment(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Path segment;

    String usage = "Usage: ParseSegment segment [-noFilter] [-noNormalize]";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    if (args.length > 1) {
      for (int i = 1; i < args.length; i++) {
        String param = args[i];

        if ("-nofilter".equalsIgnoreCase(param)) {
          getConf().setBoolean("parse.filter.urls", false);
        } else if ("-nonormalize".equalsIgnoreCase(param)) {
          getConf().setBoolean("parse.normalize.urls", false);
        }
      }
    }

    segment = new Path(args[0]);
    parse(segment);
    return 0;
  }

  /*
   * Used for Nutch REST service
   */
  public Map<String, Object> run(Map<String, Object> args, String crawlId) throws Exception {

    Map<String, Object> results = new HashMap<>();
    Path segment = null;
    if(args.containsKey(Nutch.ARG_SEGMENTS)) {
      Object seg = args.get(Nutch.ARG_SEGMENTS);
      if(seg instanceof Path) {
        segment = (Path) seg;
      }
      else if(seg instanceof String){
        segment = new Path(seg.toString());
      }
      else if(seg instanceof ArrayList) {
        String[] segmentsArray = (String[])seg;
        segment = new Path(segmentsArray[0].toString());
        	  
        if(segmentsArray.length > 1){
       	  LOG.warn("Only the first segment of segments array is used.");
        }
      }
    }
    else {
    	String segment_dir = crawlId+"/segments";
        File segmentsDir = new File(segment_dir);
        File[] segmentsList = segmentsDir.listFiles();  
        Arrays.sort(segmentsList, (f1, f2) -> {
          if(f1.lastModified()>f2.lastModified())
            return -1;
          else
            return 0;
        });
        segment = new Path(segmentsList[0].getPath());
    }
    
    if (args.containsKey("nofilter")) {
      getConf().setBoolean("parse.filter.urls", false);
    }
    if (args.containsKey("nonormalize")) {
      getConf().setBoolean("parse.normalize.urls", false);
    }
    parse(segment);
    results.put(Nutch.VAL_RESULT, Integer.toString(0));
    return results;
  }
}
