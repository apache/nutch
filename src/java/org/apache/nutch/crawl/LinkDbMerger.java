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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

/**
 * This tool merges several LinkDb-s into one, optionally filtering URLs through
 * the current URLFilters, to skip prohibited URLs and links.
 * 
 * <p>
 * It's possible to use this tool just for filtering - in that case only one
 * LinkDb should be specified in arguments.
 * </p>
 * <p>
 * If more than one LinkDb contains information about the same URL, all inlinks
 * are accumulated, but only at most <code>linkdb.max.inlinks</code> inlinks will
 * ever be added.
 * </p>
 * <p>
 * If activated, URLFilters will be applied to both the target URLs and to any
 * incoming link URL. If a target URL is prohibited, all inlinks to that target
 * will be removed, including the target URL. If some of incoming links are
 * prohibited, only they will be removed, and they won't count when checking the
 * above-mentioned maximum limit.
 * 
 * @author Andrzej Bialecki
 */
public class LinkDbMerger extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public LinkDbMerger() {

  }

  public LinkDbMerger(Configuration conf) {
    setConf(conf);
  }

  public static class LinkDbMergeReducer extends 
      Reducer<Text, Inlinks, Text, Inlinks> {

    private int maxInlinks;

    public void setup(Reducer<Text, Inlinks, Text, Inlinks>.Context context) {
      Configuration conf = context.getConfiguration();
      maxInlinks = conf.getInt("linkdb.max.inlinks", 10000);
    }

    public void reduce(Text key, Iterable<Inlinks> values, Context context)
        throws IOException, InterruptedException {

      Inlinks result = new Inlinks();

      for (Inlinks inlinks : values) {

        int end = Math.min(maxInlinks - result.size(), inlinks.size());
        Iterator<Inlink> it = inlinks.iterator();
        int i = 0;
        while (it.hasNext() && i++ < end) {
          result.add(it.next());
        }
      }
      if (result.size() == 0)
        return;
      context.write(key, result);

    }
  }

  public void close() throws IOException {
  }

  public void merge(Path output, Path[] dbs, boolean normalize, boolean filter)
      throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("LinkDb merge: starting at " + sdf.format(start));

    Job job = createMergeJob(getConf(), output, normalize, filter);
    for (int i = 0; i < dbs.length; i++) {
      FileInputFormat.addInputPath(job, new Path(dbs[i], LinkDb.CURRENT_NAME));
    }

    try {
      boolean success = job.waitForCompletion(true);
      if (!success) {
        String message = "LinkDbMerge job did not succeed, job status:"
            + job.getStatus().getState() + ", reason: "
            + job.getStatus().getFailureInfo();
        LOG.error(message);
        throw new RuntimeException(message);
      }
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      LOG.error("LinkDbMerge job failed: {}", e.getMessage());
      throw e;
    }
    FileSystem fs = output.getFileSystem(getConf());
    fs.mkdirs(output);
    fs.rename(FileOutputFormat.getOutputPath(job), new Path(output,
        LinkDb.CURRENT_NAME));

    long end = System.currentTimeMillis();
    LOG.info("LinkDb merge: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  public static Job createMergeJob(Configuration config, Path linkDb,
      boolean normalize, boolean filter) throws IOException {
    Path newLinkDb = new Path(linkDb,
        "merge-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    Job job = NutchJob.getInstance(config);
    job.setJobName("linkdb merge " + linkDb);

    Configuration conf = job.getConfiguration();
    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setMapperClass(LinkDbFilter.class);
    conf.setBoolean(LinkDbFilter.URL_NORMALIZING, normalize);
    conf.setBoolean(LinkDbFilter.URL_FILTERING, filter);
    job.setJarByClass(LinkDbMerger.class);
    job.setReducerClass(LinkDbMergeReducer.class);

    FileOutputFormat.setOutputPath(job, newLinkDb);
    job.setOutputFormatClass(MapFileOutputFormat.class);
    conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Inlinks.class);

    // https://issues.apache.org/jira/browse/NUTCH-1069
    conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

    return job;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new LinkDbMerger(),
        args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err
          .println("Usage: LinkDbMerger <output_linkdb> <linkdb1> [<linkdb2> <linkdb3> ...] [-normalize] [-filter]");
      System.err.println("\toutput_linkdb\toutput LinkDb");
      System.err
          .println("\tlinkdb1 ...\tinput LinkDb-s (single input LinkDb is ok)");
      System.err
          .println("\t-normalize\tuse URLNormalizer on both fromUrls and toUrls in linkdb(s) (usually not needed)");
      System.err
          .println("\t-filter\tuse URLFilters on both fromUrls and toUrls in linkdb(s)");
      return -1;
    }
    Path output = new Path(args[0]);
    ArrayList<Path> dbs = new ArrayList<>();
    boolean normalize = false;
    boolean filter = false;
    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-filter")) {
        filter = true;
      } else if (args[i].equals("-normalize")) {
        normalize = true;
      } else
        dbs.add(new Path(args[i]));
    }
    try {
      merge(output, dbs.toArray(new Path[dbs.size()]), normalize, filter);
      return 0;
    } catch (Exception e) {
      LOG.error("LinkDbMerger: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

}
