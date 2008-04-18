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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.*;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.*;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

/* Parse content in a segment. */
public class ParseSegment extends Configured implements Tool, Mapper<WritableComparable, Content, Text, ParseImpl>, Reducer<Text, Writable, Text, Writable> {

  public static final Log LOG = LogFactory.getLog(Parser.class);
  
  private ScoringFilters scfilters;
  
  public ParseSegment() {
    this(null);
  }
  
  public ParseSegment(Configuration conf) {
    super(conf);
  }

  public void configure(JobConf job) {
    setConf(job);
    this.scfilters = new ScoringFilters(job);
  }

  public void close() {}
  
  private Text newKey = new Text();

  public void map(WritableComparable key, Content content,
                  OutputCollector<Text, ParseImpl> output, Reporter reporter)
    throws IOException {
    // convert on the fly from old UTF8 keys
    if (key instanceof UTF8) {
      newKey.set(key.toString());
      key = newKey;
    }
    
    int status =
      Integer.parseInt(content.getMetadata().get(Nutch.FETCH_STATUS_KEY));
    if (status != CrawlDatum.STATUS_FETCH_SUCCESS) {
      // content not fetched successfully, skip document
      LOG.debug("Skipping " + key + " as content is not fetched successfully");
      return;
    }

    ParseResult parseResult = null;
    try {
      parseResult = new ParseUtil(getConf()).parse(content);
    } catch (Exception e) {
      LOG.warn("Error parsing: " + key + ": " + StringUtils.stringifyException(e));
      return;
    }

    for (Entry<Text, Parse> entry : parseResult) {
      Text url = entry.getKey();
      Parse parse = entry.getValue();
      ParseStatus parseStatus = parse.getData().getStatus();
      
      if (!parseStatus.isSuccess()) {
        LOG.warn("Error parsing: " + key + ": " + parseStatus);
        parse = parseStatus.getEmptyParse(getConf());
      }

      // pass segment name to parse data
      parse.getData().getContentMeta().set(Nutch.SEGMENT_NAME_KEY, 
                                           getConf().get(Nutch.SEGMENT_NAME_KEY));

      // compute the new signature
      byte[] signature = 
        SignatureFactory.getSignature(getConf()).calculate(content, parse); 
      parse.getData().getContentMeta().set(Nutch.SIGNATURE_KEY, 
          StringUtil.toHexString(signature));
      
      try {
        scfilters.passScoreAfterParsing(url, content, parse);
      } catch (ScoringFilterException e) {
        if (LOG.isWarnEnabled()) {
          e.printStackTrace(LogUtil.getWarnStream(LOG));
          LOG.warn("Error passing score: "+ url +": "+e.getMessage());
        }
      }
      output.collect(url, new ParseImpl(new ParseText(parse.getText()), 
                                        parse.getData(), parse.isCanonical()));
    }
  }

  public void reduce(Text key, Iterator<Writable> values,
                     OutputCollector<Text, Writable> output, Reporter reporter)
    throws IOException {
    output.collect(key, (Writable)values.next()); // collect first value
  }

  public void parse(Path segment) throws IOException {

    if (LOG.isInfoEnabled()) {
      LOG.info("Parse: starting");
      LOG.info("Parse: segment: " + segment);
    }

    JobConf job = new NutchJob(getConf());
    job.setJobName("parse " + segment);

    job.setInputPath(new Path(segment, Content.DIR_NAME));
    job.set(Nutch.SEGMENT_NAME_KEY, segment.getName());
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(ParseSegment.class);
    job.setReducerClass(ParseSegment.class);
    
    job.setOutputPath(segment);
    job.setOutputFormat(ParseOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ParseImpl.class);

    JobClient.runJob(job);
    if (LOG.isInfoEnabled()) { LOG.info("Parse: done"); }
  }


  public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(NutchConfiguration.create(), new ParseSegment(), args);
	System.exit(res);
  }
	  
  public int run(String[] args) throws Exception {
    Path segment;

    String usage = "Usage: ParseSegment segment";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }      
    segment = new Path(args[0]);
    parse(segment);
    return 0;
  }
}
