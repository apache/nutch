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

package org.apache.nutch.crawl;

import org.apache.nutch.io.*;
import org.apache.nutch.fs.*;
import org.apache.nutch.mapred.*;
import org.apache.nutch.util.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.net.*;

import java.io.*;
import java.util.*;
import java.util.logging.*;

/* Parse content in a segment. */
public class ParseSegment 
  extends NutchConfigured implements Mapper, Reducer, OutputFormat {

  public static final Logger LOG =
    LogFormatter.getLogger(Parser.class.getName());

  public static final String SEGMENT_NAME_KEY = "nutch.segment.name";

  private float interval;
  private String segmentName;

  private UrlNormalizer urlNormalizer = UrlNormalizerFactory.getNormalizer();
        
  public ParseSegment() { super(null); }

  public ParseSegment(NutchConf conf) {
    super(conf);
  }

  public void configure(JobConf job) {
    interval = job.getFloat("db.default.fetch.interval", 30f);
    segmentName = job.get(SEGMENT_NAME_KEY);
  }

  public void map(WritableComparable key, Writable value,
                  OutputCollector output, Reporter reporter)
    throws IOException {
    Content content = (Content)value;

    Parse parse = null;
    ParseStatus status;
    try {
      Parser parser = ParserFactory.getParser(content.getContentType(),
                                              content.getBaseUrl());
      parse = parser.getParse(content);
      status = parse.getData().getStatus();
    } catch (Exception e) {
      status = new ParseStatus(e);
    }

    if (status.isSuccess()) {
      parse.getData().getMetadata().setProperty(SEGMENT_NAME_KEY, segmentName);
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

  public RecordWriter getRecordWriter(NutchFileSystem fs, JobConf job,
                                      String name) throws IOException {
    File text =
      new File(new File(job.getOutputDir(), ParseText.DIR_NAME), name);
    File data =
      new File(new File(job.getOutputDir(), ParseData.DIR_NAME), name);
    File crawl =
      new File(new File(job.getOutputDir(), CrawlDatum.PARSE_DIR_NAME), name);
    
    final MapFile.Writer textOut =
      new MapFile.Writer(fs, text.toString(), UTF8.class, ParseText.class);
    
    final MapFile.Writer dataOut =
      new MapFile.Writer(fs, data.toString(), UTF8.class, ParseData.class);
    
    final SequenceFile.Writer crawlOut =
      new SequenceFile.Writer(fs, crawl.toString(),
                              UTF8.class, CrawlDatum.class);
    
    return new RecordWriter() {

        public void write(WritableComparable key, Writable value)
          throws IOException {
          
          Parse parse = (Parse)value;
          
          textOut.append(key, new ParseText(parse.getText()));
          dataOut.append(key, parse.getData());

          // collect outlinks for subsequent db update
          Outlink[] links = parse.getData().getOutlinks();
          for (int i = 0; i < links.length; i++) {
            String toUrl = links[i].getToUrl();
            try {
              toUrl = urlNormalizer.normalize(toUrl); // normalize the url
              toUrl = URLFilters.filter(toUrl);   // filter the url
            } catch (Exception e) {
              toUrl = null;
            }
            if (toUrl != null)
              crawlOut.append(new UTF8(toUrl),
                              new CrawlDatum(CrawlDatum.STATUS_LINKED,
                                             interval));
          }
        }
        
        public void close() throws IOException {
          textOut.close();
          dataOut.close();
          crawlOut.close();
        }
        
      };
    
  }      

  public void parse(File segment) throws IOException {
    LOG.info("Parse: starting");
    LOG.info("Parse: segment: " + segment);

    JobConf job = new JobConf(getConf());

    job.set(SEGMENT_NAME_KEY, segment.getName());

    job.setInputDir(new File(segment, Content.DIR_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(Content.class);
    job.setMapperClass(ParseSegment.class);
    job.setReducerClass(ParseSegment.class);
    
    job.setOutputDir(segment);
    job.setOutputFormat(ParseSegment.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(ParseImpl.class);

    JobClient.runJob(job);
    LOG.info("Parse: done");
  }


  public static void main(String[] args) throws Exception {
    File segment;

    String usage = "Usage: ParseSegment segment";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
      
    segment = new File(args[0]);

    ParseSegment parseSegment = new ParseSegment(NutchConf.get());
    parseSegment.parse(segment);
  }
}
