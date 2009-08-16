package org.apache.nutch.parse;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.ValueFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatumHbase;
import org.apache.nutch.crawl.Signature;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.URLUtil;
import org.apache.nutch.util.hbase.HbaseColumn;
import org.apache.nutch.util.hbase.WebTableRow;
import org.apache.nutch.util.hbase.WebTableColumns;
import org.apache.nutch.util.hbase.TableUtil;

public class TableParser
extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable>
implements Tool {

  public static final Log LOG = LogFactory.getLog(TableParser.class);

  public static final String PARSE_MARK_STR = "__prsmrk__";

  public static final byte[] PARSE_MARK = Bytes.toBytes("__prsmrk__");

  private static final Collection<HbaseColumn> COLUMNS = new HashSet<HbaseColumn>();

  private Configuration conf;

  static {
    COLUMNS.add(new HbaseColumn(WebTableColumns.STATUS));
    COLUMNS.add(new HbaseColumn(WebTableColumns.CONTENT));
    COLUMNS.add(new HbaseColumn(WebTableColumns.CONTENT_TYPE));
    COLUMNS.add(new HbaseColumn(WebTableColumns.SIGNATURE));
    COLUMNS.add(new HbaseColumn(WebTableColumns.METADATA, Fetcher.FETCH_MARK));
  }

  private ParseUtil parseUtil;
  private Signature sig;
  private URLFilters filters;
  private URLNormalizers normalizers;
  private int maxOutlinks;
  private boolean ignoreExternalLinks;
  private HTable table;

  @Override
  public void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    parseUtil = new ParseUtil(conf);
    sig = SignatureFactory.getSignature(conf);
    filters = new URLFilters(conf);
    normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_OUTLINK);
    final int maxOutlinksPerPage = conf.getInt("db.max.outlinks.per.page", 100);
    maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE
        : maxOutlinksPerPage;
    ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);
    table = new HTable(conf.get(TableInputFormat.INPUT_TABLE));
  }

  @Override
  public void map(ImmutableBytesWritable key, Result result, Context context)
  throws IOException {
    final WebTableRow row = new WebTableRow(result);
    final String url = TableUtil.unreverseUrl(Bytes.toString(key.get()));
    final byte status = row.getStatus();
    if (status != CrawlDatumHbase.STATUS_FETCHED) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping " + url + " as status is: " +
            CrawlDatumHbase.getName(status));
      }
      return;
    }

    Parse parse;
    try {
      parse = parseUtil.parse(url, row);
    } catch (final Exception e) {
      LOG.warn("Error parsing: " + url + ": " + StringUtils.stringifyException(e));
      return;
    }

    final byte[] signature = sig.calculate(row, parse);

    final ParseStatus pstatus = parse.getParseStatus();
    row.setParseStatus(pstatus);
    if (pstatus.isSuccess()) {
      if (pstatus.getMinorCode() == ParseStatus.SUCCESS_REDIRECT) {
        String newUrl = pstatus.getMessage();
        final int refreshTime = Integer.parseInt(pstatus.getArgs()[1]);
        newUrl = normalizers.normalize(newUrl,
            URLNormalizers.SCOPE_FETCHER);
        try {
          newUrl = filters.filter(newUrl);
        } catch (URLFilterException e) {
          return; // TODO: is this correct
        }
        if (newUrl == null || newUrl.equals(url)) {
          final String reprUrl = URLUtil.chooseRepr(url, newUrl,
              refreshTime < Fetcher.PERM_REFRESH_TIME);
          try {
            final String reversedUrl = TableUtil.reverseUrl(reprUrl);
            final ImmutableBytesWritable newKey =
              new ImmutableBytesWritable(reversedUrl.getBytes());
            final Put put = new Put(newKey.get());
            if (!reprUrl.equals(url)) {
              put.add(WebTableColumns.REPR_URL, null, Bytes.toBytes(reprUrl));
            }
            put.add(WebTableColumns.METADATA,
                Fetcher.REDIRECT_DISCOVERED, TableUtil.YES_VAL);
            table.put(put);
          } catch (MalformedURLException e) {
            // ignore
          }
        }
      } else {
        row.setText(parse.getText());
        row.setTitle(parse.getTitle());
        final byte[] prevSig = row.getSignature();
        if (prevSig != null) {
          row.setPrevSignature(prevSig);
        }
        row.setSignature(signature);
        row.deleteAllOutlinks();
        final Outlink[] outlinks = parse.getOutlinks();
        final int count = 0;
        String fromHost;
        if (ignoreExternalLinks) {
          try {
            fromHost = new URL(url).getHost().toLowerCase();
          } catch (final MalformedURLException e) {
            fromHost = null;
          }
        } else {
          fromHost = null;
        }
        for (int i = 0; count < maxOutlinks && i < outlinks.length; i++) {
          String toUrl = outlinks[i].getToUrl();
          toUrl = normalizers.normalize(toUrl, URLNormalizers.SCOPE_OUTLINK);
          try {
            toUrl = filters.filter(toUrl);
          } catch (final URLFilterException e) {
            continue;
          }
          if (toUrl == null) {
            continue;
          }
          String toHost;
          if (ignoreExternalLinks) {
            try {
              toHost = new URL(toUrl).getHost().toLowerCase();
            } catch (final MalformedURLException e) {
              toHost = null;
            }
            if (toHost == null || !toHost.equals(fromHost)) { // external links
              continue; // skip it
            }
          }

          row.addOutlink(new Outlink(toUrl, outlinks[i].getAnchor()));
        }
        row.putMeta(PARSE_MARK, TableUtil.YES_VAL);
      }
    }

    row.makeRowMutation().commit(table);
  }

  @Override
  public void cleanup(Context context) throws IOException {
    table.close();
  }

  private Scan makeScan(Job job, boolean restart) {
    Configuration conf = job.getConfiguration();
    final Collection<HbaseColumn> columns = new HashSet<HbaseColumn>(COLUMNS);
    final ParserFactory parserFactory = new ParserFactory(conf);
    columns.addAll(parserFactory.getColumns());
    columns.addAll(SignatureFactory.getColumns(conf));
    final HtmlParseFilters parseFilters = new HtmlParseFilters(conf);
    columns.addAll(parseFilters.getColumns());
    Scan scan = TableUtil.createScanFromColumns(columns);
    List<Filter> filters = new ArrayList<Filter>();
    if (!restart) {
      filters.add(new ValueFilter(WebTableColumns.METADATA, PARSE_MARK,
          CompareOp.NOT_EQUAL, TableUtil.YES_VAL, false));
    }
    filters.add(new ValueFilter(WebTableColumns.METADATA, Fetcher.FETCH_MARK,
        CompareOp.EQUAL, TableUtil.YES_VAL, true));
    scan.setFilter(new FilterList(Operator.MUST_PASS_ALL, filters));
    return scan;
  }


  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public void parse(String table, boolean restart) throws Exception {

    LOG.info("TableParser: starting");
    LOG.info("TableParser: segment: " + table);

    final Job job = new NutchJob(getConf(), "parse " + table);

    Scan scan = makeScan(job, restart);
    TableMapReduceUtil.initTableMapperJob(table, scan,
        TableParser.class, ImmutableBytesWritable.class,
        ImmutableBytesWritable.class, job);
    TableMapReduceUtil.initTableReducerJob(table, IdentityTableReducer.class, job);
    job.waitForCompletion(true);
    LOG.info("TableParser: done");
  }

  public int run(String[] args) throws Exception {
    final String usage = "Usage: TableParser <webtable> [-restart]";

    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }

    boolean restart = false;
    if (args.length >= 2 && "-restart".equals(args[1])) {
      restart = true;
    }

    parse(args[0], restart);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(), new TableParser(), args);
    System.exit(res);
  }

}
