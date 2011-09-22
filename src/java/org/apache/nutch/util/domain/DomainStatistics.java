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

package org.apache.nutch.util.domain;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.URLUtil;

/**
 * Extracts some very basic statistics about domains from the crawldb 
 */
public class DomainStatistics
extends MapReduceBase
implements Tool, Mapper<Text, CrawlDatum, Text, LongWritable>,
           Reducer<Text, LongWritable, LongWritable, Text> {

  private static final Logger LOG = LoggerFactory.getLogger(DomainStatistics.class);
  
  private static final Text FETCHED_TEXT = new Text("FETCHED");
  private static final Text NOT_FETCHED_TEXT = new Text("NOT_FETCHED");
  
  public static enum MyCounter {FETCHED, NOT_FETCHED, EMPTY_RESULT};
  
  private static final int MODE_HOST = 1;
  private static final int MODE_DOMAIN = 2;
  private static final int MODE_SUFFIX = 3;
  
  private int mode = 0;
  
  private Configuration conf;
  
  public int run(String[] args) throws IOException {
    if (args.length < 3) {
      System.out.println("usage: DomainStatistics inputDirs outDir host|domain|suffix [numOfReducer]");
      return 1;
    }
    String inputDir = args[0];
    String outputDir = args[1];
    int numOfReducers = 1;
    
    if (args.length > 3) {
      numOfReducers = Integer.parseInt(args[3]);
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("DomainStatistics: starting at " + sdf.format(start));

    JobConf job = new NutchJob(getConf());
    job.setJobName("Domain statistics");

    int mode = 0;
    if(args[2].equals("host"))
      mode = MODE_HOST;
    else if(args[2].equals("domain"))
      mode = MODE_DOMAIN;
    else if(args[2].equals("suffix"))
      mode = MODE_SUFFIX;
    job.setInt("domain.statistics.mode", mode);
    
    String[] inputDirsSpecs = inputDir.split(",");
    for (int i = 0; i < inputDirsSpecs.length; i++) {
      FileInputFormat.addInputPath(job, new Path(inputDirsSpecs[i]));
    }

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(DomainStatistics.class);
    FileOutputFormat.setOutputPath(job, new Path(outputDir));
    job.setOutputFormat(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setReducerClass(DomainStatistics.class);
    job.setCombinerClass(DomainStatisticsCombiner.class);
    job.setNumReduceTasks(numOfReducers);
    
    JobClient.runJob(job);
    
    long end = System.currentTimeMillis();
    LOG.info("DomainStatistics: finished at " + sdf.format(end) + ", elapsed: " + TimingUtil.elapsedTime(start, end));
    return 0;
  }

  @Override
  public void configure(JobConf job) {
    super.configure(job);
    mode = job.getInt("domain.statistics.mode", MODE_DOMAIN);
  }
  

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public void map(Text urlText, CrawlDatum datum,
      OutputCollector<Text, LongWritable> output, Reporter reporter)
  throws IOException {
    
    if(datum.getStatus() == CrawlDatum.STATUS_DB_FETCHED 
        || datum.getStatus() == CrawlDatum.STATUS_FETCH_SUCCESS) {
      try {
        URL url = new URL(urlText.toString());
        String out = null;
        switch (mode) {
          case MODE_HOST:
            out = url.getHost();
            break;
          case MODE_DOMAIN:
            out = URLUtil.getDomainName(url);
            break;
          case MODE_SUFFIX:
            out = URLUtil.getDomainSuffix(url).getDomain();
            break;
        }
        if(out.trim().equals("")) {
          LOG.info("url : " + url);
          reporter.incrCounter(MyCounter.EMPTY_RESULT, 1);
        }
        
        output.collect(new Text(out), new LongWritable(1));
      } catch (Exception ex) { }
      reporter.incrCounter(MyCounter.FETCHED, 1);
      output.collect(FETCHED_TEXT, new LongWritable(1));
    }
    else {
      reporter.incrCounter(MyCounter.NOT_FETCHED, 1);
      output.collect(NOT_FETCHED_TEXT, new LongWritable(1));
    }
  }

  public void reduce(Text key, Iterator<LongWritable> values,
      OutputCollector<LongWritable, Text> output, Reporter reporter)
  throws IOException {
    
    long total = 0;
    
    while(values.hasNext()) {
      LongWritable val = values.next();
      total += val.get();
    }
    //invert output 
    output.collect(new LongWritable(total), key);
  }
    
  
  public static class DomainStatisticsCombiner extends MapReduceBase
  implements Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text key, Iterator<LongWritable> values,
        OutputCollector<Text, LongWritable> output, Reporter reporter)
    throws IOException {
      long total = 0;
      
      while(values.hasNext()) {
        LongWritable val = values.next();
        total += val.get();
      } 
      output.collect(key, new LongWritable(total));
    }

  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(NutchConfiguration.create(), new DomainStatistics(), args);
  }
  
}
