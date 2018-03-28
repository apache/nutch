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
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
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
import org.apache.nutch.storage.Duplicate;
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

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  private static final Utf8 REINDEX = new Utf8("-reindex");

  static {
    FIELDS.add(WebPage.Field.SIGNATURE);
    FIELDS.add(WebPage.Field.PARSE_STATUS);
    FIELDS.add(WebPage.Field.SCORE);
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.METADATA);
  }

  public static class IndexerMapper extends
      GoraMapper<String, WebPage, String, NutchDocument> {
    public IndexUtil indexUtil;
    public DataStore<String, WebPage> store;
    public DataStore<String, Duplicate> duplicateStore;
    public IndexWriters writers;

    protected Utf8 batchId;
    protected boolean deduplicate;
    protected boolean commit;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      batchId = new Utf8(
          conf.get(GeneratorJob.BATCH_ID, Nutch.ALL_BATCH_ID_STR));
      indexUtil = new IndexUtil(conf);
      commit = conf.getBoolean(SolrConstants.COMMIT_INDEX, true);
      deduplicate = conf.getBoolean(Nutch.DEDUPLICATE, false);
      try {
        store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
        if (deduplicate) {
          duplicateStore = StorageUtils.createWebStore(
              conf, String.class, Duplicate.class);
        }
        writers = new IndexWriters(conf);
        writers.describe();
        writers.open(conf);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }

    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      store.close();
      if (deduplicate) {
        duplicateStore.close();
      }
      if (commit) {
        writers.commit();
      }
      writers.close();
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
      String url = TableUtil.unreverseUrl(key);
      if (mark == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping " + url + "; not updated on db yet");
        }
        return;
      }
      
      if (deduplicate) {
        Duplicate duplicate = duplicateStore.get(
            new String(page.getSignature().array()));
        if (duplicate.getOriginal() != null && !url.equals(duplicate.getOriginal())) {
          return;
        } else {
          StringBuilder duplicates = new StringBuilder("");
          for (CharSequence duplicateUrl : duplicate.getUrls()) {
            String duplicateKey = TableUtil.reverseUrl(duplicateUrl.toString());
            if (!key.equals(duplicateKey)) {
              duplicates.append(duplicateUrl);
              duplicates.append(Nutch.DUPLICATE_URLS_SPLITTER);
              writers.delete(duplicateKey);
            }
          }
          // attach the duplicates to the page as metadata
          page.getMetadata().put(Nutch.DUPLICATE_URLS_KEY, ByteBuffer.wrap(duplicates.toString().getBytes()));
        }
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
  
  public IndexingJob() {}

  public IndexingJob(Configuration conf) {
    setConf(conf);
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
    LOG.info("IndexingJob: starting");

    String batchId = (String) args.get(Nutch.ARG_BATCH);
    boolean deduplicate = (boolean) args.get(Nutch.ARG_DEDUPLICATE);

    Configuration conf = getConf();
    conf.set(GeneratorJob.BATCH_ID, batchId);
    conf.setBoolean(Nutch.DEDUPLICATE, deduplicate);

    Job job = NutchJob.getInstance(conf, "Indexer");
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

    LOG.info("IndexingJob: done.");
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

  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err
          .println("Usage: IndexingJob (<batchId> | -all | -reindex) [-crawlId <id>] (-deduplicate)");
      return -1;
    }
    
    boolean deduplicate = false;

    if (args.length == 3 && "-crawlId".equals(args[1])) {
      getConf().set(Nutch.CRAWL_ID_KEY, args[2]);
    }
    if ("-deduplicate".equals(args[args.length-1])) {
      deduplicate = true;
    }
    try {
      run(ToolUtil.toArgMap(Nutch.ARG_BATCH, args[0], Nutch.ARG_DEDUPLICATE, deduplicate));
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
