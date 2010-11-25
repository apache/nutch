package org.apache.nutch.crawl;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.ToolUtil;
import org.apache.gora.mapreduce.StringComparator;

public class DbUpdaterJob extends NutchTool implements Tool {

  public static final Logger LOG = LoggerFactory.getLogger(DbUpdaterJob.class);


  private static final Collection<WebPage.Field> FIELDS =
    new HashSet<WebPage.Field>();

  static {
    FIELDS.add(WebPage.Field.OUTLINKS);
    FIELDS.add(WebPage.Field.INLINKS);
    FIELDS.add(WebPage.Field.STATUS);
    FIELDS.add(WebPage.Field.PREV_SIGNATURE);
    FIELDS.add(WebPage.Field.SIGNATURE);
    FIELDS.add(WebPage.Field.MARKERS);
    FIELDS.add(WebPage.Field.METADATA);
    FIELDS.add(WebPage.Field.RETRIES_SINCE_FETCH);
    FIELDS.add(WebPage.Field.FETCH_TIME);
    FIELDS.add(WebPage.Field.MODIFIED_TIME);
    FIELDS.add(WebPage.Field.FETCH_INTERVAL);
    FIELDS.add(WebPage.Field.PREV_FETCH_TIME);
    FIELDS.add(WebPage.Field.PREV_SIGNATURE);
  }

  public DbUpdaterJob() {

  }

  public DbUpdaterJob(Configuration conf) {
    setConf(conf);
  }
    
  public Map<String,Object> run(Map<String,Object> args) throws Exception {
    String crawlId = (String)args.get(Nutch.ARG_CRAWL);
    numJobs = 1;
    currentJobNum = 0;
    currentJob = new NutchJob(getConf(), "update-table");
    if (crawlId != null) {
      currentJob.getConfiguration().set(Nutch.CRAWL_ID_KEY, crawlId);
    }
    //job.setBoolean(ALL, updateAll);
    ScoringFilters scoringFilters = new ScoringFilters(getConf());
    HashSet<WebPage.Field> fields = new HashSet<WebPage.Field>(FIELDS);
    fields.addAll(scoringFilters.getFields());
    // TODO: Figure out why this needs to be here
    currentJob.getConfiguration().setClass("mapred.output.key.comparator.class",
        StringComparator.class, RawComparator.class);
    StorageUtils.initMapperJob(currentJob, fields, String.class,
        NutchWritable.class, DbUpdateMapper.class);
    StorageUtils.initReducerJob(currentJob, DbUpdateReducer.class);
    currentJob.waitForCompletion(true);
    ToolUtil.recordJobStatus(null, currentJob, results);
    return results;
  }
  
  private int updateTable(String crawlId) throws Exception {
    LOG.info("DbUpdaterJob: starting");
    run(ToolUtil.toArgMap(Nutch.ARG_CRAWL, crawlId));
    LOG.info("DbUpdaterJob: done");
    return 0;
  }

  public int run(String[] args) throws Exception {
    String crawlId = null;
    if (args.length == 2 && "-crawlId".equals(args[0])) {
      crawlId = args[1];
    }
    return updateTable(crawlId);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new DbUpdaterJob(), args);
    System.exit(res);
  }

}
