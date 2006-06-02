/**
 * Copyright 2006 The Apache Software Foundation
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

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.nutch.net.URLFilters;
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
public class CrawlDbMerger extends Configured {
  private static final Logger LOG = Logger.getLogger(CrawlDbMerger.class.getName());

  public static class Merger extends MapReduceBase implements Reducer {
    private URLFilters filters = null;
    MapWritable meta = new MapWritable();

    public void close() throws IOException {}

    public void configure(JobConf conf) {
      if (conf.getBoolean("crawldb.merger.urlfilters", false))
        filters = new URLFilters(conf);
    }

    public void reduce(WritableComparable key, Iterator values, OutputCollector output, Reporter reporter)
            throws IOException {
      if (filters != null) {
        try {
          if (filters.filter(((UTF8) key).toString()) == null)
            return;
        } catch (Exception e) {
          LOG.fine("Can't filter " + key + ": " + e);
        }
      }
      CrawlDatum res = null;
      long resTime = 0L;
      meta.clear();
      while (values.hasNext()) {
        CrawlDatum val = (CrawlDatum) values.next();
        if (res == null) {
          res = val;
          resTime = res.getFetchTime() - Math.round(res.getFetchInterval() * 3600 * 24 * 1000);
          meta.putAll(res.getMetaData());
          continue;
        }
        // compute last fetch time, and pick the latest
        long valTime = val.getFetchTime() - Math.round(val.getFetchInterval() * 3600 * 24 * 1000);
        if (valTime > resTime) {
          // collect all metadata, newer values override older values
          meta.putAll(val.getMetaData());
          res = val;
          resTime = res.getFetchTime() - Math.round(res.getFetchInterval() * 3600 * 24 * 1000);
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

  public CrawlDbMerger(Configuration conf) {
    super(conf);
  }

  public void merge(Path output, Path[] dbs, boolean filter) throws Exception {
    JobConf job = createMergeJob(getConf(), output);
    job.setBoolean("crawldb.merger.urlfilters", filter);
    for (int i = 0; i < dbs.length; i++) {
      job.addInputPath(new Path(dbs[i], CrawlDatum.DB_DIR_NAME));
    }
    JobClient.runJob(job);
    FileSystem fs = FileSystem.get(getConf());
    fs.mkdirs(output);
    fs.rename(job.getOutputPath(), new Path(output, CrawlDatum.DB_DIR_NAME));
  }

  public static JobConf createMergeJob(Configuration conf, Path output) {
    Path newCrawlDb = new Path("crawldb-merge-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(conf);
    job.setJobName("crawldb merge " + output);

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(CrawlDatum.class);

    job.setReducerClass(Merger.class);

    job.setOutputPath(newCrawlDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(CrawlDatum.class);

    return job;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("CrawlDbMerger output_crawldb crawldb1 [crawldb2 crawldb3 ...] [-filter]");
      System.err.println("\toutput_crawldb\toutput CrawlDb");
      System.err.println("\tcrawldb1 ...\tinput CrawlDb-s");
      System.err.println("\t-filter\tuse URLFilters on urls in the crawldb(s)");
      return;
    }
    Configuration conf = NutchConfiguration.create();
    Path output = new Path(args[0]);
    ArrayList dbs = new ArrayList();
    boolean filter = false;
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-filter")) {
        filter = true;
        continue;
      }
      dbs.add(new Path(args[i]));
    }
    CrawlDbMerger merger = new CrawlDbMerger(conf);
    merger.merge(output, (Path[]) dbs.toArray(new Path[dbs.size()]), filter);
  }
}
