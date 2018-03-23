/*******************************************************************************
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
 ******************************************************************************/

package org.apache.nutch.indexer;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.gora.filter.Filter;
import org.apache.gora.filter.FilterOp;
import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.Duplicate;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;
import org.apache.nutch.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeduplicatorJob extends NutchTool implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.SIGNATURE);
    FIELDS.add(WebPage.Field.BASE_URL);
    FIELDS.add(WebPage.Field.MARKERS);
  }
  
  public static class DeduplicatorMapper extends GoraMapper<String, WebPage, Text, Duplicate> {
    
    @Override
    protected void map(String key, WebPage page, Context context) throws IOException, InterruptedException {
      if (page.getSignature() != null) {
        Duplicate duplicate = Duplicate.newBuilder().build();
        List<CharSequence> urls = new ArrayList<>();
        urls.add(page.getBaseUrl());
        duplicate.setURLs(urls);
        Text signature = new Text();
        signature.set(new String(page.getSignature().array()));
        context.write(signature, duplicate);
      }
    }
    
  }
  
  public static class DeduplicatorReducer extends GoraReducer<Text, Duplicate, String, Duplicate> {
    public DataStore<String, Duplicate> datastore;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      try {
        datastore = StorageUtils.createWebStore(conf, String.class, Duplicate.class);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      datastore.close();
    }
    
    @Override
    protected void reduce(Text key, Iterable<Duplicate> values, Context context) throws IOException, InterruptedException {
      Duplicate stored = datastore.get(key.toString());
      if (stored == null) {
        stored = Duplicate.newBuilder().build();
      }
      List<CharSequence> urls = stored.getURLs();
      for (Duplicate duplicate : values) {
        for (CharSequence url  : duplicate.getURLs()) {
          if (!urls.contains(url)) {
            urls.add(url);
          }
        }
      }
      stored.setURLs(urls);
      datastore.put(key.toString(), stored);
    }
  }
  
  public DeduplicatorJob() {
  }

  public DeduplicatorJob(Configuration conf) {
    setConf(conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    String crawlId = null;
    String batchId = null;

    String usage = "Usage: DeduplicatorJob (<batchId> | -all) [-crawlId <id>] "
        + "    <batchId>     - crawl identifier returned by Generator, or -all for all \n \t \t    generated batchId-s\n"
        + "    -crawlId <id> - the id with which to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)\n";

    if (args.length == 0) {
      System.err.println(usage);
      return -1;
    }
    
    batchId = args[0];
    if (!batchId.equals("-all") && batchId.startsWith("-")) {
      System.err.println(usage);
      return -1;
    }

    for (int i = 1; i < args.length; i++) {
      if ("-crawlId".equals(args[i])) {
        crawlId = args[++i];
        getConf().set(Nutch.CRAWL_ID_KEY, crawlId);
      } else {
        throw new IllegalArgumentException("arg " + args[i] + " not recognized");
      }
    }
    return deduplicate(crawlId, batchId);
  }
  
  private MapFieldValueFilter<String, WebPage> getBatchIdFilter(String batchId) {
    if (batchId.equals(Nutch.ALL_CRAWL_ID.toString())) {
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

  @Override
  public Map<String, Object> run(Map<String, Object> args) throws Exception {
    String crawlId = (String) args.get(Nutch.ARG_CRAWL);
    String batchId = (String) args.get(Nutch.ARG_BATCH);
    
    if (batchId == null) {
      batchId = Nutch.ALL_BATCH_ID_STR;
    }
    getConf().set(GeneratorJob.BATCH_ID, batchId);
    
    numJobs = 1;
    currentJobNum = 0;
    
    HashSet<WebPage.Field> fields = new HashSet<WebPage.Field>(FIELDS);

    currentJob = NutchJob.getInstance(getConf(), "deduplicate");
    if (crawlId != null) {
      currentJob.getConfiguration().set(Nutch.CRAWL_ID_KEY, crawlId);
    }
    
    Filter<String, WebPage> batchIdFilter = getBatchIdFilter(batchId);
    
    StorageUtils.initMapperJob(currentJob, fields, Text.class, Duplicate.class, DeduplicatorMapper.class, batchIdFilter);
    
    DataStore<String, Duplicate> duplicateWebPageStore = StorageUtils.createWebStore(getConf(), String.class, Duplicate.class);
    GoraReducer.initReducerJob(currentJob, duplicateWebPageStore, DeduplicatorReducer.class);
    GoraOutputFormat.setOutput(currentJob, duplicateWebPageStore, true);
    
    currentJob.waitForCompletion(true);
    ToolUtil.recordJobStatus(null, currentJob, results);
    return results;
  }
  
  public int deduplicate(String crawlId, String batchId) throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("DeduplicatorJob: starting at " + sdf.format(start));
    
    if (batchId.equals(Nutch.ALL_BATCH_ID_STR)) {
      LOG.info("DeduplicatorJob: deduplicating all");
    } else {
      LOG.info("DeduplicatorJob: batchId: "+ batchId);
    }
    
    run(ToolUtil.toArgMap(Nutch.ARG_CRAWL, crawlId, Nutch.ARG_BATCH, batchId));

    long finish = System.currentTimeMillis();
    LOG.info("DeduplicatorJob: finished at " + sdf.format(finish)
        + ", time elapsed: " + TimingUtil.elapsedTime(start, finish));
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(NutchConfiguration.create(), new DeduplicatorJob(), args));
  }

}