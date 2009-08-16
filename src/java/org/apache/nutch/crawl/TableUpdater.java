package org.apache.nutch.crawl;

import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.parse.TableParser;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.hbase.HbaseColumn;
import org.apache.nutch.util.hbase.WebTableColumns;
import org.apache.nutch.util.hbase.TableUtil;

public class TableUpdater extends Configured
implements Tool {

  public static final Log LOG = LogFactory.getLog(TableUpdater.class);


  private static final Collection<HbaseColumn> COLUMNS = new HashSet<HbaseColumn>();

  static {
    COLUMNS.add(new HbaseColumn(WebTableColumns.OUTLINKS));
    COLUMNS.add(new HbaseColumn(WebTableColumns.INLINKS));
    COLUMNS.add(new HbaseColumn(WebTableColumns.STATUS));
    COLUMNS.add(new HbaseColumn(WebTableColumns.METADATA,  TableParser.PARSE_MARK));
    COLUMNS.add(new HbaseColumn(WebTableColumns.METADATA,  Fetcher.REDIRECT_DISCOVERED));
    COLUMNS.add(new HbaseColumn(WebTableColumns.RETRIES));
    COLUMNS.add(new HbaseColumn(WebTableColumns.FETCH_TIME));
    COLUMNS.add(new HbaseColumn(WebTableColumns.MODIFIED_TIME));
    COLUMNS.add(new HbaseColumn(WebTableColumns.FETCH_INTERVAL));
    COLUMNS.add(new HbaseColumn(WebTableColumns.PREV_FETCH_TIME));
    COLUMNS.add(new HbaseColumn(WebTableColumns.PREV_SIGNATURE));
  }

  private void updateTable(String table) throws Exception {
    LOG.info("TableUpdater: starting");
    LOG.info("TableUpdater: table: " + table);
    Job job = new NutchJob(getConf(), "update-table " + table);
    //job.setBoolean(ALL, updateAll);
    job.setJobName("update-table " + table);
    ScoringFilters scoringFilters = new ScoringFilters(getConf());
    HashSet<HbaseColumn> columns = new HashSet<HbaseColumn>(COLUMNS);
    columns.addAll(scoringFilters.getColumns());
    TableMapReduceUtil.initTableMapperJob(table, TableUtil.createScanFromColumns(columns), 
        TableUpdateMapper.class, ImmutableBytesWritable.class, 
        NutchWritable.class, job);
    TableMapReduceUtil.initTableReducerJob(table, TableUpdateReducer.class, job);

    job.waitForCompletion(true);
    LOG.info("TableUpdater: done");
  }

  public int run(String[] args) throws Exception {
    String usage = "Usage: TableUpdater <webtable>";

    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }

    updateTable(args[0]);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new TableUpdater(), args);
    System.exit(res);
  }

}
