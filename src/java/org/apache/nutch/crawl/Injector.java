package org.apache.nutch.crawl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.hbase.HbaseColumn;
import org.apache.nutch.util.hbase.WebTableRow;
import org.apache.nutch.util.hbase.WebTableColumns;
import org.apache.nutch.util.hbase.TableUtil;

public class Injector
implements Tool {

  public static final Log LOG = LogFactory.getLog(Injector.class);
  
  private static final String INJECT_KEY_STR = "__injkey__";
  public static final byte[] INJECT_KEY =
    Bytes.toBytes(INJECT_KEY_STR);
  
  private static final Set<HbaseColumn> COLUMNS = new HashSet<HbaseColumn>();
  
  static {
    COLUMNS.add(new HbaseColumn(WebTableColumns.METADATA, INJECT_KEY));
    COLUMNS.add(new HbaseColumn(WebTableColumns.STATUS));
  }
  
  private Configuration conf;
  
  public static class UrlMapper
  extends Mapper<LongWritable, Text, Text, Text> {
    private URLNormalizers urlNormalizers;
    private URLFilters filters;
    private HTable table;
    private HBaseConfiguration hbaseConf;

    @Override
    public void map(LongWritable key, Text value, Context context) 
    throws IOException {
      if (table == null) {
        throw new IOException("Can not connect to hbase table");
      }
      String url = value.toString();
      String reversedUrl;
      try {
        url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INJECT);
        url = filters.filter(url);
        if (url == null) {
          return;
        }
        reversedUrl = TableUtil.reverseUrl(url);
      } catch (Exception e) {
        LOG.warn("Skipping " + url + ":" + e);
        return;
      }

      Put put = new Put(Bytes.toBytes(reversedUrl));
      put.add(WebTableColumns.METADATA, INJECT_KEY, TableUtil.YES_VAL);

      table.put(put);
    }

    @Override
    public void setup(Context context) {  
      Configuration conf = context.getConfiguration();
      urlNormalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_INJECT);
      filters = new URLFilters(conf);
      hbaseConf = new HBaseConfiguration();
      try {
        table = new HTable(hbaseConf, conf.get("input.table") );
      } catch (IOException e) {
        e.printStackTrace(LogUtil.getFatalStream(LOG));
      }

    }
    
    @Override
    public void cleanup(Context context) throws IOException {
      table.close();
    }

  }
  
  public static class InjectorMapper
  extends TableMapper<Text, Text> {
    private HTable table;
    private float scoreInjected;
    private FetchSchedule schedule;
    private ScoringFilters scoringFilters;
    
    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      schedule = FetchScheduleFactory.getFetchSchedule(conf);
      table = new HTable(conf.get(TableInputFormat.INPUT_TABLE));
      scoreInjected = conf.getFloat("db.score.injected", 1.0f);
      scoringFilters = new ScoringFilters(conf);
    }
    
    @Override
    public void map(ImmutableBytesWritable key, Result result, Context context)
    throws IOException {
      WebTableRow row = new WebTableRow(result);
      row.deleteMeta(INJECT_KEY);
      if (!row.hasColumn(WebTableColumns.STATUS, null)) {
        String url = TableUtil.unreverseUrl(Bytes.toString(key.get()));
        // this is a new column so add necessary fields
        row.setStatus(CrawlDatumHbase.STATUS_UNFETCHED);
        schedule.initializeSchedule(url, row);
        try {
          scoringFilters.injectedScore(url, row);
        } catch (ScoringFilterException e) {
          row.setScore(scoreInjected);
        }
      }
      row.makeRowMutation().commit(table);
    }
    
    @Override
    public void cleanup(Context context) throws IOException {
      table.close();
    }
  }

  public boolean inject(String table, Path urlDir)
  throws IOException, InterruptedException, ClassNotFoundException {

    LOG.info("Injector: starting");
    LOG.info("Injector: urlDir: " + urlDir);

    getConf().setLong("injector.current.time", System.currentTimeMillis());
    getConf().set("input.table", table);
    Job job = new NutchJob(getConf(), "inject-hbase-p1 " + urlDir);
    FileInputFormat.addInputPath(job, urlDir);
    job.setMapperClass(UrlMapper.class);
    TableMapReduceUtil.initTableReducerJob(table,
        IdentityTableReducer.class, job);
    job.setOutputFormatClass(NullOutputFormat.class);

    if (!job.waitForCompletion(true)) {
      LOG.warn("Injecting new users failed!");
      return false;
    }
    job = new NutchJob(getConf(), "inject-hbase-p2 " + urlDir);

    Scan scan = TableUtil.createScanFromColumns(COLUMNS);
    scan.setFilter(new ValueFilter(WebTableColumns.METADATA, INJECT_KEY, ValueFilter.CompareOp.EQUAL, TableUtil.YES_VAL, true));
    TableMapReduceUtil.initTableMapperJob(table,
        scan, InjectorMapper.class, Text.class, Text.class, job);
    TableMapReduceUtil.initTableReducerJob(table,
        IdentityTableReducer.class, job);
    if (job.waitForCompletion(true)) {
      LOG.info("Injector: done");
      return true;
    }
    return false;
  }
  

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: Injector <webtable> <url_dir>");
      return -1;
    }
    try {
      if (inject(args[0], new Path(args[1]))) {
        return 0;
      }
      return -1;
    } catch (Exception e) {
      LOG.fatal("Injector: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(),
        new Injector(), args);
    System.exit(res);
  }
}
