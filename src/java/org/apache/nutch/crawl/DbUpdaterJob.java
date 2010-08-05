package org.apache.nutch.crawl;

import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.gora.mapreduce.StringComparator;

public class DbUpdaterJob extends Configured
implements Tool {

  public static final Log LOG = LogFactory.getLog(DbUpdaterJob.class);


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

  private int updateTable() throws Exception {
    LOG.info("DbUpdaterJob: starting");
    Job job = new NutchJob(getConf(), "update-table");
    //job.setBoolean(ALL, updateAll);
    ScoringFilters scoringFilters = new ScoringFilters(getConf());
    HashSet<WebPage.Field> fields = new HashSet<WebPage.Field>(FIELDS);
    fields.addAll(scoringFilters.getFields());
    // TODO: Figure out why this needs to be here
    job.getConfiguration().setClass("mapred.output.key.comparator.class",
        StringComparator.class, RawComparator.class);
    StorageUtils.initMapperJob(job, fields, String.class,
        NutchWritable.class, DbUpdateMapper.class);
    StorageUtils.initReducerJob(job, DbUpdateReducer.class);

    boolean success = job.waitForCompletion(true);
    if (!success){
    	LOG.info("DbUpdaterJob: failed");
    	return -1;
    }
    LOG.info("DbUpdaterJob: done");
    return 0;
  }

  public int run(String[] args) throws Exception {
	return updateTable();
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new DbUpdaterJob(), args);
    System.exit(res);
  }

}
