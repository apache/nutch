/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.LogFormatter;
import org.apache.nutch.protocol.*;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.util.*;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;
import java.util.logging.*;

/* Parse content in a segment. */
public class ParseSegment extends Configured implements Mapper, Reducer {

  public static final Logger LOG =
    LogFormatter.getLogger(Parser.class.getName());
  
  public ParseSegment() {
    this(null);
  }
  
  public ParseSegment(Configuration conf) {
    super(conf);
  }

  public void configure(JobConf job) {
    setConf(job);
  }

  public void close() {}

  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter)
    throws IOException {
    Content content = (Content)value;

    Parse parse = null;
    ParseStatus status;
    try {
      parse = new ParseUtil(getConf()).parse(content);
      status = parse.getData().getStatus();
    } catch (Exception e) {
      status = new ParseStatus(e);
    }

    // compute the new signature
    byte[] signature = SignatureFactory.getSignature(getConf()).calculate(content, parse);
    content.getMetadata().set(Fetcher.SIGNATURE_KEY, StringUtil.toHexString(signature));
    
    if (status.isSuccess()) {
      output.collect(key, new ParseImpl(parse.getText(), parse.getData()));
    } else {
      LOG.warning("Error parsing: "+key+": "+status.toString());
    }
  }

  public void reduce(WritableComparable key, Iterator values,
                     OutputCollector output, Reporter reporter)
    throws IOException {
    output.collect(key, (Writable)values.next()); // collect first value
  }

  public void parse(Path segment) throws IOException {
    LOG.info("Parse: starting");
    LOG.info("Parse: segment: " + segment);

    JobConf job = new NutchJob(getConf());
    job.setJobName("parse " + segment);

    job.setInputPath(new Path(segment, Content.DIR_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(Content.class);
    job.setMapperClass(ParseSegment.class);
    job.setReducerClass(ParseSegment.class);
    
    job.setOutputPath(segment);
    job.setOutputFormat(ParseOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(ParseImpl.class);

    JobClient.runJob(job);
    LOG.info("Parse: done");
  }


  public static void main(String[] args) throws Exception {
    Path segment;

    String usage = "Usage: ParseSegment segment";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    segment = new Path(args[0]);

    ParseSegment parseSegment = new ParseSegment(NutchConfiguration.create());
    parseSegment.parse(segment);
  }
}
