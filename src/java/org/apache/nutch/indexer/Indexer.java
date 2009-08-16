package org.apache.nutch.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.ValueFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.indexer.lucene.LuceneWriter;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.hbase.HbaseColumn;
import org.apache.nutch.util.hbase.WebTableRow;
import org.apache.nutch.util.hbase.WebTableColumns;
import org.apache.nutch.util.hbase.TableUtil;

public class Indexer
extends TableMapper<ImmutableBytesWritable, WebTableRow> 
implements Tool {

  public static final String DONE_NAME = "index.done";
  
  public static final Log LOG = LogFactory.getLog(Indexer.class);

  private static final Collection<HbaseColumn> COLUMNS = new HashSet<HbaseColumn>();
  
  public static final byte[] INDEX_MARK = Bytes.toBytes("__idxmrk__");
  
  private Configuration conf;

  static {
    COLUMNS.add(new HbaseColumn(WebTableColumns.SIGNATURE));
    COLUMNS.add(new HbaseColumn(WebTableColumns.PARSE_STATUS));
    COLUMNS.add(new HbaseColumn(WebTableColumns.SCORE));
    COLUMNS.add(new HbaseColumn(WebTableColumns.METADATA, INDEX_MARK));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  @Override
  public void map(ImmutableBytesWritable key, Result result, Context context)
  throws IOException, InterruptedException {
    WebTableRow row = new WebTableRow(result);

    ParseStatus pstatus = row.getParseStatus();
    if (!pstatus.isSuccess() || 
        pstatus.getMinorCode() == ParseStatus.SUCCESS_REDIRECT) {
      return;   // filter urls not parsed
    }

    context.write(key, row);
  }

  public static Collection<HbaseColumn> getColumns(Job job) {
    Configuration conf = job.getConfiguration();
    Collection<HbaseColumn> columns = new HashSet<HbaseColumn>(COLUMNS);
    IndexingFilters filters = new IndexingFilters(conf);
    columns.addAll(filters.getColumns());
    ScoringFilters scoringFilters = new ScoringFilters(conf);
    columns.addAll(scoringFilters.getColumns());
    return columns;  
  }
  
  public static Job createIndexJob(Configuration conf, String jobName,
      String table, boolean reindex) throws IOException {
    Job job = new NutchJob(conf, jobName);
    Scan scan = TableUtil.createScanFromColumns(getColumns(job));
    List<Filter> filters = new ArrayList<Filter>();
    if (!reindex) {
      filters.add(
          new ValueFilter(WebTableColumns.METADATA, INDEX_MARK,
              CompareOp.NOT_EQUAL, TableUtil.YES_VAL, false));
    }
    filters.add(
        new ValueFilter(WebTableColumns.PARSE_STATUS, null,
                        CompareOp.GREATER_OR_EQUAL, new byte[] { (byte)0 }, true));
    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, filters);
    scan.setFilter(filterList);
    TableMapReduceUtil.initTableMapperJob(table,
        scan,
        Indexer.class, ImmutableBytesWritable.class,
        WebTableRow.class, job);

    job.setReducerClass(IndexerReducer.class);
    job.setOutputFormatClass(IndexerOutputFormat.class);
    return job;
  }

  private void index(Path indexDir, String table, boolean reindex) throws Exception {
    LOG.info("IndexerHbase: starting");
    LOG.info("IndexerHbase: table: " + table);

    LuceneWriter.addFieldOptions("segment", LuceneWriter.STORE.YES, LuceneWriter.INDEX.NO, getConf());
    LuceneWriter.addFieldOptions("digest", LuceneWriter.STORE.YES, LuceneWriter.INDEX.NO, getConf());
    LuceneWriter.addFieldOptions("boost", LuceneWriter.STORE.YES, LuceneWriter.INDEX.NO, getConf());

    NutchIndexWriterFactory.addClassToConf(getConf(), LuceneWriter.class);

    Job job = createIndexJob(getConf(), "index " + table, table, reindex); 

    FileOutputFormat.setOutputPath(job, indexDir);

    job.waitForCompletion(true);
    FileOutputFormat.setOutputPath(job, indexDir);

    job.waitForCompletion(true);
    LOG.info("IndexerHbase: done");
  }

  public int run(String[] args) throws Exception {
    String usage = "Usage: IndexerHbase <index> <webtable> [-reindex]";

    if (args.length < 2) {
      System.err.println(usage);
      System.exit(-1);
    }
    
    boolean reindex = false;
    if (args.length >= 3 && "-reindex".equals(args[2])) {
      reindex = true;
    }

    index(new Path(args[0]), args[1], reindex);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new Indexer(), args);
    System.exit(res);
  }

}
