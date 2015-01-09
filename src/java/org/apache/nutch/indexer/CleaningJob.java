/*
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
package org.apache.nutch.indexer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.StringComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.ToolUtil;

public class CleaningJob extends NutchTool implements Tool {

  public static final String ARG_COMMIT = "commit";
  public static final Logger LOG = LoggerFactory.getLogger(CleaningJob.class);
  private Configuration conf;

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.STATUS);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Collection<WebPage.Field> getFields(Job job) {
    Configuration conf = job.getConfiguration();
    Collection<WebPage.Field> columns = new HashSet<WebPage.Field>(FIELDS);
    IndexCleaningFilters filters = new IndexCleaningFilters(conf);
    columns.addAll(filters.getFields());
    return columns;
  }

  public static class CleanMapper extends
      GoraMapper<String, WebPage, String, WebPage> {

    private IndexCleaningFilters filters;

    @Override
    protected void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      filters = new IndexCleaningFilters(conf);
    }

    @Override
    public void map(String key, WebPage page, Context context)
        throws IOException, InterruptedException {
      try {
        if (page.getStatus() == CrawlStatus.STATUS_GONE
            || filters.remove(key, page)) {
          context.write(key, page);
        }
      } catch (IndexingException e) {
        LOG.warn("Error indexing " + key + ": " + e);
      }
    }
  }

  public static class CleanReducer extends
      Reducer<String, WebPage, NullWritable, NullWritable> {
    private int numDeletes = 0;
    private static final int NUM_MAX_DELETE_REQUEST = 1000;
    private boolean commit;
    IndexWriters writers = null;

    @Override
    public void setup(Context job) throws IOException {
      Configuration conf = job.getConfiguration();
      writers = new IndexWriters(conf);
      try {
        writers.open(conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      commit = conf.getBoolean(ARG_COMMIT, false);
    }

    public void reduce(String key, Iterable<WebPage> values, Context context)
        throws IOException {
      writers.delete(key);
      numDeletes++;
      context.getCounter("SolrClean", "DELETED").increment(1);
    }

    @Override
    public void cleanup(Context context) throws IOException {
      writers.close();
      if (numDeletes > 0 && commit) {
        writers.commit();
      }
      LOG.info("CleaningJob: deleted a total of " + numDeletes + " documents");
    }
  }

  @Override
  public Map<String, Object> run(Map<String, Object> args) throws Exception {
    getConf().setBoolean(ARG_COMMIT, (Boolean) args.get(ARG_COMMIT));
    currentJob = new NutchJob(getConf(), "CleaningJob");
    currentJob.getConfiguration().setClass(
        "mapred.output.key.comparator.class", StringComparator.class,
        RawComparator.class);

    Collection<WebPage.Field> fields = getFields(currentJob);
    StorageUtils.initMapperJob(currentJob, fields, String.class, WebPage.class,
        CleanMapper.class);
    currentJob.setReducerClass(CleanReducer.class);
    currentJob.setOutputFormatClass(NullOutputFormat.class);
    currentJob.waitForCompletion(true);
    ToolUtil.recordJobStatus(null, currentJob, results);
    return results;
  }

  public int delete(boolean commit) throws Exception {
    LOG.info("CleaningJob: starting");
    run(ToolUtil.toArgMap(ARG_COMMIT, commit));
    LOG.info("CleaningJob: done");
    return 0;
  }

  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: CleaningJob [-crawlId <id>] [-noCommit]");
      return 1;
    }

    boolean commit = true;
    if (args.length == 3 && args[2].equals("-noCommit")) {
      commit = false;
    }
    if (args.length == 3 && "-crawlId".equals(args[0])) {
      getConf().set(Nutch.CRAWL_ID_KEY, args[1]);
    }

    return delete(commit);
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(NutchConfiguration.create(), new CleaningJob(),
        args);
    System.exit(result);
  }

}
