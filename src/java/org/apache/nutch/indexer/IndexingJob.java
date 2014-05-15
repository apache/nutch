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

import org.apache.avro.util.Utf8;
import org.apache.gora.filter.FilterOp;
import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.StringComparator;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.indexer.solr.SolrConstants;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingJob extends NutchTool implements Tool {

  public static Logger LOG = LoggerFactory.getLogger(IndexingJob.class);

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  private static final Utf8 REINDEX = new Utf8("-reindex");

  static {
    FIELDS.add(WebPage.Field.SIGNATURE);
    FIELDS.add(WebPage.Field.PARSE_STATUS);
    FIELDS.add(WebPage.Field.SCORE);
    FIELDS.add(WebPage.Field.MARKERS);
  }

  public static class IndexerMapper extends
      GoraMapper<String, WebPage, String, NutchDocument> {
    public IndexUtil indexUtil;
    public DataStore<String, WebPage> store;

    protected Utf8 batchId;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      batchId = new Utf8(
          conf.get(GeneratorJob.BATCH_ID, Nutch.ALL_BATCH_ID_STR));
      indexUtil = new IndexUtil(conf);
      try {
        store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }

    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      store.close();
    };

    @Override
    public void map(String key, WebPage page, Context context)
        throws IOException, InterruptedException {
      ParseStatus pstatus = page.getParseStatus();
      if (pstatus == null || !ParseStatusUtils.isSuccess(pstatus)
          || pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
        return; // filter urls not parsed
      }

      Utf8 mark = Mark.UPDATEDB_MARK.checkMark(page);
      if (mark == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping " + TableUtil.unreverseUrl(key)
              + "; not updated on db yet");
        }
        return;
      }

      NutchDocument doc = indexUtil.index(key, page);
      if (doc == null) {
        return;
      }
      if (mark != null) {
        Mark.INDEX_MARK.putMark(page, Mark.UPDATEDB_MARK.checkMark(page));
        store.put(key, page);
      }
      context.write(key, doc);
      context.getCounter("IndexerJob", "DocumentCount").increment(1);
    }
  }

  private static Collection<WebPage.Field> getFields(Job job) {
    Configuration conf = job.getConfiguration();
    Collection<WebPage.Field> columns = new HashSet<WebPage.Field>(FIELDS);
    IndexingFilters filters = new IndexingFilters(conf);
    columns.addAll(filters.getFields());
    ScoringFilters scoringFilters = new ScoringFilters(conf);
    columns.addAll(scoringFilters.getFields());
    return columns;
  }

  @Override
  public Map<String, Object> run(Map<String, Object> args) throws Exception {
    String batchId = (String) args.get(Nutch.ARG_BATCH);

    Configuration conf = getConf();
    conf.set(GeneratorJob.BATCH_ID, batchId);

    Job job = new NutchJob(conf, "Indexer");
    // TODO: Figure out why this needs to be here
    job.getConfiguration().setClass("mapred.output.key.comparator.class",
        StringComparator.class, RawComparator.class);

    Collection<WebPage.Field> fields = getFields(job);
    MapFieldValueFilter<String, WebPage> batchIdFilter = getBatchIdFilter(batchId);
    StorageUtils.initMapperJob(job, fields, String.class, NutchDocument.class,
        IndexerMapper.class, batchIdFilter);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(IndexerOutputFormat.class);

    job.waitForCompletion(true);
    ToolUtil.recordJobStatus(null, job, results);
    return results;
  }

  private MapFieldValueFilter<String, WebPage> getBatchIdFilter(String batchId) {
    if (batchId.equals(REINDEX.toString())
        || batchId.equals(Nutch.ALL_CRAWL_ID.toString())) {
      return null;
    }
    MapFieldValueFilter<String, WebPage> filter = new MapFieldValueFilter<String, WebPage>();
    filter.setFieldName(WebPage.Field.MARKERS.toString());
    filter.setFilterOp(FilterOp.EQUALS);
    filter.setFilterIfMissing(true);
    filter.setMapKey(Mark.UPDATEDB_MARK.getName());
    filter.getOperands().add(new Utf8(batchId));
    return filter;
  }

  public void index(String batchId) throws Exception {
    LOG.info("IndexingJob: starting");

    run(ToolUtil.toArgMap(Nutch.ARG_BATCH, batchId));
    // NOW PASSED ON THE COMMAND LINE AS A HADOOP PARAM
    // do the commits once and for all the reducers in one go
    // getConf().set(SolrConstants.SERVER_URL,solrUrl);

    IndexWriters writers = new IndexWriters(getConf());
    LOG.info(writers.describe());
    
    writers.open(getConf());
    if (getConf().getBoolean(SolrConstants.COMMIT_INDEX, true)) {
      writers.commit();
    }
    LOG.info("IndexingJob: done.");
  }

  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err
          .println("Usage: IndexingJob (<batchId> | -all | -reindex) [-crawlId <id>]");
      return -1;
    }

    if (args.length == 3 && "-crawlId".equals(args[1])) {
      getConf().set(Nutch.CRAWL_ID_KEY, args[2]);
    }
    try {
      index(args[0]);
      return 0;
    } catch (final Exception e) {
      LOG.error("SolrIndexerJob: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new IndexingJob(), args);
    System.exit(res);
  }
}
