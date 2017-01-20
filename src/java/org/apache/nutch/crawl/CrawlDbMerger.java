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
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

// Commons Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

/**
 * This tool merges several CrawlDb-s into one, optionally filtering URLs
 * through the current URLFilters, to skip prohibited pages.
 * 
 * <p>
 * It's possible to use this tool just for filtering - in that case only one
 * CrawlDb should be specified in arguments.
 * </p>
 * <p>
 * If more than one CrawlDb contains information about the same URL, only the
 * most recent version is retained, as determined by the value of
 * {@link org.apache.nutch.crawl.CrawlDatum#getFetchTime()}. However, all
 * metadata information from all versions is accumulated, with newer values
 * taking precedence over older values.
 * 
 * @author Andrzej Bialecki
 */
public class CrawlDbMerger extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public static class Merger extends MapReduceBase implements
      Reducer<Text, CrawlDatum, Text, CrawlDatum> {
    private org.apache.hadoop.io.MapWritable meta;
    private CrawlDatum res = new CrawlDatum();
    private FetchSchedule schedule;

    public void close() throws IOException {
    }

    public void configure(JobConf conf) {
      schedule = FetchScheduleFactory.getFetchSchedule(conf);
    }

    public void reduce(Text key, Iterator<CrawlDatum> values,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
      long resTime = 0L;
      boolean resSet = false;
      meta = new org.apache.hadoop.io.MapWritable();
      while (values.hasNext()) {
        CrawlDatum val = values.next();
        if (!resSet) {
          res.set(val);
          resSet = true;
          resTime = schedule.calculateLastFetchTime(res);
          for (Entry<Writable, Writable> e : res.getMetaData().entrySet()) {
            meta.put(e.getKey(), e.getValue());
          }
          continue;
        }
        // compute last fetch time, and pick the latest
        long valTime = schedule.calculateLastFetchTime(val);
        if (valTime > resTime) {
          // collect all metadata, newer values override older values
          for (Entry<Writable, Writable> e : val.getMetaData().entrySet()) {
            meta.put(e.getKey(), e.getValue());
          }
          res.set(val);
          resTime = valTime;
        } else {
          // insert older metadata before newer
          for (Entry<Writable, Writable> e : meta.entrySet()) {
            val.getMetaData().put(e.getKey(), e.getValue());
          }
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

  public void merge(Path output, Path[] dbs, boolean normalize, boolean filter)
      throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("CrawlDb merge: starting at " + sdf.format(start));

    JobConf job = createMergeJob(getConf(), output, normalize, filter);
    for (int i = 0; i < dbs.length; i++) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Adding " + dbs[i]);
      }
      FileInputFormat.addInputPath(job, new Path(dbs[i], CrawlDb.CURRENT_NAME));
    }
    JobClient.runJob(job);
    FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(output))
      fs.delete(output, true);
    fs.mkdirs(output);
    fs.rename(FileOutputFormat.getOutputPath(job), new Path(output,
        CrawlDb.CURRENT_NAME));
    long end = System.currentTimeMillis();
    LOG.info("CrawlDb merge: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  public static JobConf createMergeJob(Configuration conf, Path output,
      boolean normalize, boolean filter) {
    Path newCrawlDb = new Path("crawldb-merge-"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(conf);
    job.setJobName("crawldb merge " + output);

    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(CrawlDbFilter.class);
    job.setBoolean(CrawlDbFilter.URL_FILTERING, filter);
    job.setBoolean(CrawlDbFilter.URL_NORMALIZING, normalize);
    job.setReducerClass(Merger.class);

    FileOutputFormat.setOutputPath(job, newCrawlDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    return job;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new CrawlDbMerger(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err
          .println("Usage: CrawlDbMerger <output_crawldb> <crawldb1> [<crawldb2> <crawldb3> ...] [-normalize] [-filter]");
      System.err.println("\toutput_crawldb\toutput CrawlDb");
      System.err
          .println("\tcrawldb1 ...\tinput CrawlDb-s (single input CrawlDb is ok)");
      System.err
          .println("\t-normalize\tuse URLNormalizer on urls in the crawldb(s) (usually not needed)");
      System.err.println("\t-filter\tuse URLFilters on urls in the crawldb(s)");
      return -1;
    }
    Path output = new Path(args[0]);
    ArrayList<Path> dbs = new ArrayList<>();
    boolean filter = false;
    boolean normalize = false;
    FileSystem fs = FileSystem.get(getConf());
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-filter")) {
        filter = true;
        continue;
      } else if (args[i].equals("-normalize")) {
        normalize = true;
        continue;
      }
      final Path dbPath = new Path(args[i]);
      if (fs.exists(dbPath))
        dbs.add(dbPath);
    }
    try {
      merge(output, dbs.toArray(new Path[dbs.size()]), normalize, filter);
      return 0;
    } catch (Exception e) {
      LOG.error("CrawlDb merge: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
}
