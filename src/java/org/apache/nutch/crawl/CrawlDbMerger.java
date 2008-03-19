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

package org.apache.nutch.crawl;

import java.io.IOException;
import java.util.*;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/**
 * This tool merges several CrawlDb-s into one, optionally filtering
 * URLs through the current URLFilters, to skip prohibited
 * pages.
 * 
 * <p>It's possible to use this tool just for filtering - in that case
 * only one CrawlDb should be specified in arguments.</p>
 * <p>If more than one CrawlDb contains information about the same URL,
 * only the most recent version is retained, as determined by the
 * value of {@link org.apache.nutch.crawl.CrawlDatum#getFetchTime()}.
 * However, all metadata information from all versions is accumulated,
 * with newer values taking precedence over older values.
 * 
 * @author Andrzej Bialecki
 */
public class CrawlDbMerger extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(CrawlDbMerger.class);

  public static class Merger extends MapReduceBase implements Reducer<Text, CrawlDatum, Text, CrawlDatum> {
    MapWritable meta = new MapWritable();
    private FetchSchedule schedule;

    public void close() throws IOException {}

    public void configure(JobConf conf) {
      schedule = FetchScheduleFactory.getFetchSchedule(conf);
    }

    public void reduce(Text key, Iterator<CrawlDatum> values, OutputCollector<Text, CrawlDatum> output, Reporter reporter)
            throws IOException {
      CrawlDatum res = null;
      long resTime = 0L;
      meta.clear();
      while (values.hasNext()) {
        CrawlDatum val = values.next();
        if (res == null) {
          res = val;
          resTime = schedule.calculateLastFetchTime(res);
          meta.putAll(res.getMetaData());
          continue;
        }
        // compute last fetch time, and pick the latest
        long valTime = schedule.calculateLastFetchTime(val);
        if (valTime > resTime) {
          // collect all metadata, newer values override older values
          meta.putAll(val.getMetaData());
          res = val;
          resTime = valTime ;
        } else {
          // insert older metadata before newer
          val.getMetaData().putAll(meta);
          meta = val.getMetaData();
        }
      }
      res.setMetaData(meta);
      output.collect(key, res);
    }
  }
  
  public CrawlDbMerger() {
    
  }
  
  public CrawlDbMerger(Configuration conf) {
    setConf(conf);
  }

  public void merge(Path output, Path[] dbs, boolean normalize, boolean filter) throws Exception {
    JobConf job = createMergeJob(getConf(), output, normalize, filter);
    for (int i = 0; i < dbs.length; i++) {
      job.addInputPath(new Path(dbs[i], CrawlDb.CURRENT_NAME));
    }
    JobClient.runJob(job);
    FileSystem fs = FileSystem.get(getConf());
    fs.mkdirs(output);
    fs.rename(job.getOutputPath(), new Path(output, CrawlDb.CURRENT_NAME));
  }

  public static JobConf createMergeJob(Configuration conf, Path output, boolean normalize, boolean filter) {
    Path newCrawlDb = new Path("crawldb-merge-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(conf);
    job.setJobName("crawldb merge " + output);

    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(CrawlDbFilter.class);
    job.setBoolean(CrawlDbFilter.URL_FILTERING, filter);
    job.setBoolean(CrawlDbFilter.URL_NORMALIZING, normalize);
    job.setReducerClass(Merger.class);

    job.setOutputPath(newCrawlDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    return job;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new CrawlDbMerger(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: CrawlDbMerger <output_crawldb> <crawldb1> [<crawldb2> <crawldb3> ...] [-normalize] [-filter]");
      System.err.println("\toutput_crawldb\toutput CrawlDb");
      System.err.println("\tcrawldb1 ...\tinput CrawlDb-s (single input CrawlDb is ok)");
      System.err.println("\t-normalize\tuse URLNormalizer on urls in the crawldb(s) (usually not needed)");
      System.err.println("\t-filter\tuse URLFilters on urls in the crawldb(s)");
      return -1;
    }
    Path output = new Path(args[0]);
    ArrayList<Path> dbs = new ArrayList<Path>();
    boolean filter = false;
    boolean normalize = false;
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-filter")) {
        filter = true;
        continue;
      } else if (args[i].equals("-normalize")) {
        normalize = true;
        continue;
      }
      dbs.add(new Path(args[i]));
    }
    try {
      merge(output, dbs.toArray(new Path[dbs.size()]), normalize, filter);
      return 0;
    } catch (Exception e) {
      LOG.fatal("CrawlDb merge: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
}
