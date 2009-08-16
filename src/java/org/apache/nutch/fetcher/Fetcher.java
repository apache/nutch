package org.apache.nutch.fetcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.hbase.HbaseColumn;
import org.apache.nutch.util.hbase.WebTableColumns;
import org.apache.nutch.util.hbase.TableUtil;

/** Multi-threaded fetcher.
 *
 */
public class Fetcher
implements Tool {
  
  public static final String PROTOCOL_REDIR = "protocol";
  
  public static final int PERM_REFRESH_TIME = 5;

  public static final byte[] REDIRECT_DISCOVERED =
    Bytes.toBytes("___rdrdsc__");

  public static final byte[] FETCH_MARK =
    Bytes.toBytes("__ftchmrk__");
  
  private static final Collection<HbaseColumn> COLUMNS =
    new HashSet<HbaseColumn>();
  
  static {
    COLUMNS.add(new HbaseColumn(WebTableColumns.METADATA,
                                Generator.GENERATOR_MARK));
    COLUMNS.add(new HbaseColumn(WebTableColumns.REPR_URL));
  }
  
  /**
   * <p>Mapper class for Fetcher.</p>
   * <p>
   * This class reads the random integer written by {@link GeneratorHbase} as its key
   * while outputting the actual key and value arguments through a {@link FetchEntry}
   * instance.
   * </p>
   * <p>
   * This approach (combined with the use of {@link PartitionUrlByHostHbase})
   * makes sure that Fetcher is still polite while also randomizing the key order.
   * If one host has a huge number of URLs in your table while other hosts
   * have not, {@link FetcherReducer} will not be stuck on one host but process
   * URLs from other hosts as well.
   * </p> 
   */
  public static class FetcherMapper
  extends TableMapper<ImmutableBytesWritable, FetchEntry> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value,
        Context context) throws IOException, InterruptedException {
      byte[] outKeyRaw =
        value.getValue(WebTableColumns.METADATA, Generator.GENERATOR_MARK);
      context.write(new ImmutableBytesWritable(outKeyRaw), new FetchEntry(key, value));
    }
  }
  
  public static final Log LOG = LogFactory.getLog(Fetcher.class);

  private Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  private void fetch(String table, int threads, boolean restart) throws Exception {
    LOG.info("Fetcher: starting");
    LOG.info("Fetcher: table: " + table);
    
    if (threads > 0) {
      getConf().setInt("fetcher.threads.fetch", threads);
    }
    
    Job job = new NutchJob(getConf(), "fetch " + table);
    Scan scan = TableUtil.createScanFromColumns(COLUMNS);
    List<Filter> filters = new ArrayList<Filter>();
    if (!restart) {
      filters.add(new ValueFilter(WebTableColumns.METADATA, FETCH_MARK,
          CompareOp.NOT_EQUAL, TableUtil.YES_VAL, false));
    }
    filters.add(new ValueFilter(WebTableColumns.METADATA, Generator.GENERATOR_MARK,
        CompareOp.GREATER_OR_EQUAL, new byte[] { (byte)0 }, true));
    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, filters);
    scan.setFilter(filterList);
    TableMapReduceUtil.initTableMapperJob(table, scan, FetcherMapper.class,
        ImmutableBytesWritable.class, FetchEntry.class, job);
    TableMapReduceUtil.initTableReducerJob(table,
        FetcherReducer.class, job, PartitionUrlByHost.class);
    
    job.waitForCompletion(true);
    
    LOG.info("Fetcher: done");
  }
  
  @Override
  public int run(String[] args) throws Exception {
    final String usage = "Usage: FetcherHbase <webtable> [-threads n] [-restart]";

    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }

    final String table = args[0];

    int threads = -1;
    boolean restart = false;

    for (int i = 1; i < args.length; i++) {
      if ("-threads".equals(args[i])) {
        // found -threads option
        threads =  Integer.parseInt(args[++i]);
      } else if ("-restart".equals(args[i])) {
        restart = true;
      }
    }

    fetch(table, threads, restart);              // run the Fetcher

    return 0;
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new Fetcher(), args);
    System.exit(res);
  }
}
