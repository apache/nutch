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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.*;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.*;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

/* Parse content in a segment. */
public class ParseSegment extends NutchTool implements Tool,
    Mapper<WritableComparable<?>, Content, Text, ParseImpl>,
    Reducer<Text, Writable, Text, Writable> {

  public static final Logger LOG = LoggerFactory.getLogger(ParseSegment.class);

  public static final String SKIP_TRUNCATED = "parser.skip.truncated";

  private ScoringFilters scfilters;

  private ParseUtil parseUtil;

  private boolean skipTruncated;

  public ParseSegment() {
    this(null);
  }

  public ParseSegment(Configuration conf) {
    super(conf);
  }

  public void configure(JobConf job) {
    setConf(job);
    this.scfilters = new ScoringFilters(job);
    skipTruncated = job.getBoolean(SKIP_TRUNCATED, true);
  }

  public void close() {
  }

  private Text newKey = new Text();

  public void map(WritableComparable<?> key, Content content,
      OutputCollector<Text, ParseImpl> output, Reporter reporter)
      throws IOException {
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
        parseUtil = new ParseUtil(getConf());
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

      reporter.incrCounter("ParserStatus",
          ParseStatus.majorCodes[parseStatus.getMajorCode()], 1);

      if (!parseStatus.isSuccess()) {
        LOG.warn("Error parsing: " + key + ": " + parseStatus);
        parse = parseStatus.getEmptyParse(getConf());
      }

      // pass segment name to parse data
      parse.getData().getContentMeta()
          .set(Nutch.SEGMENT_NAME_KEY, getConf().get(Nutch.SEGMENT_NAME_KEY));

      // compute the new signature
      byte[] signature = SignatureFactory.getSignature(getConf()).calculate(
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

      output.collect(
          url,
          new ParseImpl(new ParseText(parse.getText()), parse.getData(), parse
              .isCanonical()));
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

  public void reduce(Text key, Iterator<Writable> values,
      OutputCollector<Text, Writable> output, Reporter reporter)
      throws IOException {
    output.collect(key, values.next()); // collect first value
  }

  public void parse(Path segment) throws IOException {
     if (SegmentChecker.isParsed(segment, FileSystem.get(getConf()))) {
	  LOG.warn("Segment: " + segment
	  + " already parsed!! Skipped parsing this segment!!"); // NUTCH-1854
          return;
      }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("ParseSegment: starting at " + sdf.format(start));
      LOG.info("ParseSegment: segment: " + segment);
    }

    JobConf job = new NutchJob(getConf());
    job.setJobName("parse " + segment);

    FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));
    job.set(Nutch.SEGMENT_NAME_KEY, segment.getName());
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(ParseSegment.class);
    job.setReducerClass(ParseSegment.class);

    FileOutputFormat.setOutputPath(job, segment);
    job.setOutputFormat(ParseOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ParseImpl.class);

    JobClient.runJob(job);
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

    Map<String, Object> results = new HashMap<String, Object>();
    Path segment;
    if(args.containsKey(Nutch.ARG_SEGMENT)) {
    	Object seg = args.get(Nutch.ARG_SEGMENT);
    	if(seg instanceof Path) {
    		segment = (Path) seg;
    	}
    	else {
    		segment = new Path(seg.toString());
    	}
    }
    else {
    	String segment_dir = crawlId+"/segments";
        File segmentsDir = new File(segment_dir);
        File[] segmentsList = segmentsDir.listFiles();  
        Arrays.sort(segmentsList, new Comparator<File>(){
          @Override
          public int compare(File f1, File f2) {
            if(f1.lastModified()>f2.lastModified())
              return -1;
            else
              return 0;
          }      
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
